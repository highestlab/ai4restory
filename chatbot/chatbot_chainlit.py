import chainlit as cl
from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain_community.vectorstores import OracleVS
from langchain_community.embeddings import OCIGenAIEmbeddings
from langchain_community.chat_models.oci_generative_ai import ChatOCIGenAI
from langchain_community.vectorstores.utils import DistanceStrategy
import oracledb
import oci
import base64
import os
import dotenv

# === Carica variabili da .env ===
dotenv.load_dotenv()

TABLE_NAME = os.getenv("TABLE_NAME")         # → MATERIALI
USER = os.getenv("USER")                     # → AI4RESTORY
SECRET_NAME = os.getenv("SECRET_NAME")       # → pwd-DBDEVVS1-pdb2-AIRESTORY-user-v1
os.environ["CHAINLIT_AUTH_SECRET"] = os.getenv("CHAINLIT_AUTH_SECRET")

# === LLM ===
llm = ChatOCIGenAI(
    model_id="ocid1.generativeaimodel.oc1.eu-frankfurt-1.amaaaaaask7dceyatobkuq6yg3lqeqhawaj3i7pckwaoeyf2liwnzvgtp6ba",
    provider="meta",
    service_endpoint="https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com",
    compartment_id="ocid1.compartment.oc1..aaaaaaaaobswoagzkkotnusnm7arq4faj7zwmhvjer4uy2jy7apszec3ky7q",
    model_kwargs={
        "temperature": 0,
        "max_tokens": 500,
        "top_k": -1
    }
)

# === Configurazione OCI ===
config = oci.config.from_file()

# === Recupera password dal Vault ===
secrets_client = oci.secrets.SecretsClient(config)
secret_bundle = secrets_client.get_secret_bundle_by_name(
    vault_id="ocid1.vault.oc1.eu-frankfurt-1.enth2qg3aahqs.abtheljsdmogh64yuxskpycelmxk275gnwea5cjkti6a6smw6akqbdvf3dcq",
    secret_name=SECRET_NAME
)
encoded_pwd = secret_bundle.data.secret_bundle_content.content
db_password = base64.b64decode(encoded_pwd).decode("unicode_escape")

# === Connessione Oracle DB ===
conn = oracledb.connect(
    user=USER,
    password=db_password,
    dsn="130.61.223.239:1521/DBDEVVS1_pdb2.unitosubnetdevp.unitovcndev.oraclevcn.com"
)

# === Embedding Model ===
embeddings = OCIGenAIEmbeddings(
    model_id="cohere.embed-multilingual-v3.0",
    service_endpoint="https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com",
    compartment_id="ocid1.compartment.oc1..aaaaaaaaobswoagzkkotnusnm7arq4faj7zwmhvjer4uy2jy7apszec3ky7q",
)

# === Vector Store ===
vector_store = OracleVS(
    client=conn,
    embedding_function=embeddings,
    table_name=TABLE_NAME,
    distance_strategy=DistanceStrategy.COSINE
)

retriever = vector_store.as_retriever(search_kwargs={"k": 5})

# === Chainlit callbacks ===
@cl.on_chat_start
async def on_chat_start():
    qa_chain = RetrievalQA.from_chain_type(
        llm=llm,
        retriever=retriever,
        return_source_documents=True
    )
    cl.user_session.set("qa_chain", qa_chain)

@cl.on_message
async def on_message(message: cl.Message):
    chain = cl.user_session.get("qa_chain")
    response = chain(message.content)

    answer = response["result"]
    sources = response.get("source_documents", [])

    if sources:
        source_strs = [
            f"- **{doc.metadata.get('source', 'Documento')}** (Tag: {doc.metadata.get('tag', '-')})"
            for doc in sources
        ]
        answer += "\n\n📚 **Fonti**:\n" + "\n".join(source_strs)

    await cl.Message(content=answer).send()

# === Avvio Chainlit ===
if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)
