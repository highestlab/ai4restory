import oci
import pandas as pd
from io import BytesIO
import pdfplumber
from langchain.schema import Document
from langchain_community.embeddings import OCIGenAIEmbeddings
from langchain_community.vectorstores import OracleVS
from langchain_community.vectorstores.utils import DistanceStrategy
from langchain.text_splitter import RecursiveCharacterTextSplitter
import oracledb
from tqdm import tqdm
import os
import dotenv
import base64 

# Carica il file .env e leggere le variabili
dotenv.load_dotenv()
# === Connessione bucket e namespace ===
NAMESPACE = os.getenv("NAMESPACE")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# === Path delle directory nel bucket ===
BASE_PATH_STUDENTI = os.getenv("BASE_PATH_STUDENTI")

# === Connessione Oracle ===
USER = os.getenv("USER")
SECRET_NAME = os.getenv("SECRET_NAME")

# === Tabelle Vector Store ===
TABLE_NAME = os.getenv("TABLE_NAME")






config = oci.config.from_file()
    
secrets_client = oci.secrets.SecretsClient(config)
    
# Send the request to service
get_secret_bundle_by_name_response = secrets_client.get_secret_bundle_by_name(
    secret_name=SECRET_NAME,
    vault_id="ocid1.vault.oc1.eu-frankfurt-1.enth2qg3aahqs.abtheljsdmogh64yuxskpycelmxk275gnwea5cjkti6a6smw6akqbdvf3dcq",
    )
    
# Get password encrypted
coded_string = get_secret_bundle_by_name_response.data.secret_bundle_content.content

##CONNESSIONE DB

try:
    conn = oracledb.connect(
        user=USER,
        password=base64.b64decode(coded_string).decode('unicode_escape'),
        dsn="130.61.223.239:1521/DBDEVVS1_pdb2.unitosubnetdevp.unitovcndev.oraclevcn.com"
      )
    
    # MODELLO DI EMBEDDING
    embeddings = OCIGenAIEmbeddings(
        model_id="cohere.embed-multilingual-v3.0",
        service_endpoint="https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com",
        compartment_id="ocid1.compartment.oc1..aaaaaaaaobswoagzkkotnusnm7arq4faj7zwmhvjer4uy2jy7apszec3ky7q",
        )

    # Vector Store
    vector_store = OracleVS(
        client=conn,
        embedding_function=embeddings,
        table_name=TABLE_NAME,
        distance_strategy=DistanceStrategy.COSINE
    )

    # OCI client
    object_storage = oci.object_storage.ObjectStorageClient(config)

    # Carica il file Excel

    df_meta = pd.read_excel(r"C:\Users\mcaligar\Desktop\Codice\Elenco_corretto.xlsx")

    # Text splitter
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        separators=["\n\n", "\n", ".", " "]
    )

    # === Estrazione con pdfplumber ===
    all_docs = []

    for _, row in df_meta.iterrows():
        file_name = row['Source_title']
        tag = row['Tag_completo']
        object_name = row['Path']

        try:
            response = object_storage.get_object(NAMESPACE, BUCKET_NAME, object_name)
            if not hasattr(response, "data") or not hasattr(response.data, "content"):
                raise Exception(f"❌ Il file '{file_name}' non ha contenuto accessibile via .data.content")

            pdf_bytes = response.data.content


            full_text = ""

            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    # Testo normale
                    page_text = page.extract_text() or ""
                    
                    # Tabelle convertite in testo tipo CSV
                    table_texts = []
                    for table in page.extract_tables():
                        table_text = "\n".join([", ".join(row) for row in table if row])
                        table_texts.append(table_text)
                    
                    # Unisci testo + eventuali tabelle
                    combined = page_text + "\n\n" + "\n\n".join(table_texts)
                    full_text += combined + "\n\n"

            # Suddividi in chunk
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

            print(f" {file_name} → {len(chunks)} chunk")

        except Exception as e:
            print(f" Errore nel file {file_name}: {e}")

    # === Inserimento a batch nel Vector Store ===
    batch_size = 100
    for i in tqdm(range(0, len(all_docs), batch_size)):
        vector_store.add_documents(all_docs[i:i+batch_size])

    print(f"✅ Inseriti {len(all_docs)} chunk nella tabella.")

except Exception as e:
      print(f"An error occurred: {e}")

