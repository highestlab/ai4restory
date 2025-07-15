import oci
import pandas as pd
import os
import re
import spacy
from oci.pagination import list_call_get_all_results

# === CONFIG ===
NAMESPACE = os.getenv("NAMESPACE")
BUCKET_NAME = "bucket-ai4restory"

# === Carica modello NLP per italiano ===
nlp = spacy.load("it_core_news_sm")

# === Carica configurazione OCI ===
config = oci.config.from_file(
    file_location="C:/Users/mcaligar/.oci/config",
    profile_name="DEFAULT"
)
object_storage = oci.object_storage.ObjectStorageClient(config)

# Recupera namespace se non impostato
if not NAMESPACE:
    NAMESPACE = object_storage.get_namespace().data

# Estrai nomi di oggetti dal bucket
response = list_call_get_all_results(
    object_storage.list_objects,
    namespace_name=NAMESPACE,
    bucket_name=BUCKET_NAME
)
object_names = [obj.name for obj in response.data.objects]

# Mappe tipi file
PDF_TYPE_MAP = {
    "RES": "Scheda di restauro",
    "RTM": "Relazione tecnica indagini multispettrali",
    "RTS": "Relazione tecnica indagini scientifiche",
    "CAM": "Scheda di campionamento",
    "STR": "Scheda tecnica di rilevamento"
}
KEYWORD_TYPE_MAP = {
    "scheda di restauro": "Scheda di restauro",
    "relazione tecnica": "Relazione tecnica",
    "campionamento": "Scheda di campionamento",
    "rilevamento": "Scheda tecnica di rilevamento",
    "mappature": "Mappature grafiche",
    "analisi": "Analisi della specie lignea"
}
ANALYSIS_MAP = {
    "fc": "Falso colore",
    "ir": "Infrarosso",
    "ir": "Infrarosso in bianco e nero",
    "uv": "Fluorescenza ultravioletta",
    "rx": "Radiografia"
}

# Blacklist token comuni nei titoli
TITLE_BLACKLIST = {"Lampadario","Scena","Specchiera","Paravento","Scrivania"}

# Pattern cartella base: numeri + lettere + almeno 2 separatori
folder_pattern = re.compile(r'^\d+[A-Za-z]*\d{2}(?:[-_].+){2,}')

def detect_folder_base(parts):
    for p in parts[:-1]:
        if folder_pattern.match(p):
            return p
    return parts[0]

# Parsing robusto
records = []
for full_path in object_names:
    parts = full_path.split('/')
    file_name = parts[-1]
    folder = detect_folder_base(parts)

    # Estrai commessa/luogo/rest_raw
    if '_' in folder:
        fparts = folder.split('_', 2)
        commessa = fparts[0]
        luogo    = fparts[1] if len(fparts) > 1 else None
        rest_raw = fparts[2] if len(fparts) > 2 else ''
    else:
        t = folder.split('-')
        commessa = '-'.join(t[:3]) if len(t)>=3 else None
        luogo    = t[3] if len(t)>3 else None
        rest_raw = '-'.join(t[4:]) if len(t)>4 else ''

    # Anno dal numero commessa
    anno = None
    if commessa:
        m = re.search(r'(\d{4})$', commessa)
        anno = m.group(1) if m else None

    # Estrai autore con spaCy su token
    autore = None
    titolo = None
    if rest_raw:
        tokens = re.split('[-_]', rest_raw)
        # ricerca autore
        for tok in tokens:
            if tok and tok not in TITLE_BLACKLIST:
                doc = nlp(tok)
                if any(ent.label_=='PER' for ent in doc.ents):
                    autore = tok
                    break
        # se nessun autore trovato e piu nomi PERSON in rest_raw -> anonimo
        doc_full = nlp(rest_raw.replace('-', ' ').replace('_',' '))
        persons = [ent.text for ent in doc_full.ents if ent.label_=='PER']
        if len(persons)>1:
            autore = 'anonimo'
        # fallback anonimo se nessun autore e multipli token
        if autore is None and len(tokens)>1:
            autore = 'anonimo'
        # Estrai titolo rimuovendo autore e inv se presente
        tail = rest_raw
        if autore and autore!='anonimo':
            tail = rest_raw.replace(autore+'-','',1)
        # strip inventario
        m_t = re.match(r'^(.*?)(?:-inv\d.*)?$', tail)
        titolo = m_t.group(1) if m_t and m_t.group(1) else None
    
    # Tipo file
    ext = os.path.splitext(file_name)[1].lower().lstrip('.')
    tipo = 'Unknown'
    if ext=='pdf':
        parts_fn = file_name.split('_')
        code = parts_fn[1].upper() if len(parts_fn)>1 else ''
        if code in PDF_TYPE_MAP:
            tipo = PDF_TYPE_MAP[code]
        else:
            low = file_name.lower()
            found = next((v for k,v in KEYWORD_TYPE_MAP.items() if k in low), None)
            tipo = found if found else 'pdf'
    elif ext in ['jpg','jpeg','png','tif','tiff','bmp']:
        m = re.match(rf'^{re.escape(commessa)}-([PDF])\d+', file_name) if commessa else None
        if m:
            fase = m.group(1)
            tipo = {'P':'Fotografia fase restauro - Prima','D':'Fotografia fase restauro - Durante','F':'Fotografia fase restauro - Fine'}[fase]
        elif re.match(r'^M\d+', file_name):
            sub = folder.split('_')[1].lower() if '_' in folder else ''
            tipo = f"Fotografia analisi - {ANALYSIS_MAP.get(sub, sub.title())}"
        else:
            tipo = ext
    else:
        # mappa estensioni aggiuntive
        tipo = ext

    records.append({
        'Source_title':file_name,
        'Path':full_path,
        'Numero_commessa':commessa,
        'Luogo':luogo,
        'Autore':autore,
        'Titolo_opera':titolo,
        'Anno_inizio_restauro':anno,
        'Tipo_file':tipo
    })

# Esporta
df = pd.DataFrame(records)
df.to_excel('elenco_file_bucket_con_metadati_robusto_v2.xlsx', index=False)
print('✅ File Excel creato: elenco_file_bucket_con_metadati_robusto_v2.xlsx')
