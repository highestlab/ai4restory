import oci
import pandas as pd
import os

# === CONFIG ===
NAMESPACE = os.getenv("NAMESPACE")  # Inserisci il tuo se non usi .env
BUCKET_NAME = "bucket-ai4restory"

# === Carica configurazione OCI ===
config = oci.config.from_file(file_location="C:/Users/mcaligar/.oci/config", profile_name="DEFAULT")  # Adatta se necessario
object_storage = oci.object_storage.ObjectStorageClient(config)

# === Estrai tutti i nomi degli oggetti ===
from oci.pagination import list_call_get_all_results

response = list_call_get_all_results(
    object_storage.list_objects,
    namespace_name=NAMESPACE,
    bucket_name=BUCKET_NAME
)

object_names = [obj.name for obj in response.data.objects]

# === Costruisci dataframe ===
data = []

for full_path in object_names:
    file_name = full_path.split("/")[-1]  # Solo il nome del file
    data.append({
        "Source_title": file_name,
        "Path": full_path
    })

df = pd.DataFrame(data)

# === Esporta in Excel ===
output_path = "elenco_file_bucket.xlsx"
df.to_excel(output_path, index=False)
print(f"✅ File Excel creato: {output_path}")
