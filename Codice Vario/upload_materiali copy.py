import os
import base64
import dotenv
import oci
import oracledb
import pandas as pd
import pdfplumber
from io import BytesIO
from tqdm import tqdm
from PIL import Image
import pytesseract

from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OCIGenAIEmbeddings
from langchain_community.vectorstores import OracleVS
from langchain_community.vectorstores.utils import DistanceStrategy

# === Configurazione OCR (solo Windows, se serve) ===
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# === Caricamento variabili da .env ===
dotenv.load_dotenv()

NAMESPACE = os.getenv("NAMESPACE")
BUCKET_NAME = os.getenv("BUCKET_NAME")
BASE_PATH_STUDENTI = os.getenv("BASE_PATH_STUDENTI")
USER = os.getenv("USER")
SECRET_NAME = os.getenv("SECRET_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

# === Setup client OCI ===
config = oci.config.from_file()
secrets_client = oci.secrets.SecretsClient(config)

# === Recupero password da OCI Vault ===
get_secret_bundle_by_name_response = secrets_client.get_secret_bundle_by_name(
    secret_name=SECRET_NAME,
    vault_id="ocid1.vault.oc1.eu-frankfurt-1.enth2qg3aahqs.abtheljsdmogh64yuxskpycelmxk275gnwea5cjkti6a6smw6akqbdvf3dcq",
)
coded_string = get_secret_bundle_by_name_response.data.secret_bundle_content.content

# === Connessione Oracle DB ===
try:
    conn = oracledb.connect(
        user=USER,
        password=base64.b64decode(coded_string).decode("unicode_escape"),
        dsn="130.61.223.239:1521/DBDEVVS1_pdb2.unitosubnetdevp.unitovcndev.oraclevcn.com"
    )

    # === Setup embedding e vector store ===
    embeddings = OCIGenAIEmbeddings(
        model_id="cohere.embed-multilingual-v3.0",
        service_endpoint="https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com",
        compartment_id="ocid1.compartment.oc1..aaaaaaaaobswoagzkkotnusnm7arq4faj7zwmhvjer4uy2jy7apszec3ky7q",
    )

    vector_store = OracleVS(
        client=conn,
        embedding_function=embeddings,
        table_name=TABLE_NAME,
        distance_strategy=DistanceStrategy.COSINE
    )

    object_storage = oci.object_storage.ObjectStorageClient(config)

    df_meta = pd.read_excel(r"C:\Users\mcaligar\Desktop\Codice\Elenco_corretto.xlsx")

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        separators=["\n\n", "\n", ".", " "]
    )

    all_docs = []

    def extract_text_from_image(img_bytes):
        image = Image.open(BytesIO(img_bytes))
        return pytesseract.image_to_string(image, lang='eng')

    def extract_text_from_excel(xlsx_bytes):
        df = pd.read_excel(BytesIO(xlsx_bytes))
        return df.to_string(index=False)

    for _, row in df_meta.iterrows():
        file_name = row['Source_title']
        tag = row['Tag_completo']
        object_name = row['Path']

        try:
            response = object_storage.get_object(NAMESPACE, BUCKET_NAME, object_name)
            file_bytes = response.data.content
            full_text = ""

            if file_name.lower().endswith(".pdf"):
                with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                    for page in pdf.pages:
                        # Estrazione del testo della pagina
                        page_text = page.extract_text() or ""  # Gestisce il caso in cui `extract_text()` ritorna None
                        
                        table_texts = []
                        for table in page.extract_tables():
                            if table:  # Verifica che la tabella non sia vuota
                                # Rimuove righe vuote e concatena le righe
                                table_text = "\n".join([", ".join(row) for row in table if row and all(cell is not None for cell in row)])
                                table_texts.append(table_text)
                        
                        # Combina il testo estratto dalla pagina e dalle tabelle
                        combined = page_text + "\n\n" + "\n\n".join(table_texts)
                        
                        # Aggiungi il testo alla variabile full_text
                        if combined.strip():  # Aggiungi solo se il testo combinato non è vuoto
                            full_text += combined + "\n\n"

            elif file_name.lower().endswith(".xlsx"):
                full_text = extract_text_from_excel(file_bytes)

            elif file_name.lower().endswith((".jpg", ".jpeg")):
                full_text = extract_text_from_image(file_bytes)

            else:
                print(f"⚠️ Tipo file non gestito: {file_name}")
                continue

            chunks = text_splitter.split_text(full_text)

            for i, chunk in enumerate(chunks):
                doc = Document(
                    page_content=chunk,
                    metadata={
                        "source": file_name,
                        "tag": tag,
                        "chunk_id": i
                    }
                )
                all_docs.append(doc)

            print(f"✅ {file_name} → {len(chunks)} chunk")

        except Exception as e:
            print(f"❌ Errore nel file {file_name}: {e}")

    # === Upload a batch nel vector store ===
    batch_size = 100
    for i in tqdm(range(0, len(all_docs), batch_size)):
        vector_store.add_documents(all_docs[i:i + batch_size])

    print(f"✅ Inseriti {len(all_docs)} chunk nella tabella.")

except Exception as e:
    print(f"❌ Errore generale: {e}")