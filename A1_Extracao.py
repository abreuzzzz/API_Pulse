import requests
import pandas as pd
import json
import time
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# 🔐 Chaves da Omie
APP_KEY = "5519963813364"
APP_SECRET = "756d32d251ef60fb54f21a14c99aa838"  # Substitua por sua chave real

# ▶ Funções para buscar dados da Omie
def get_titulos():
    all_data = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        body = {
            "call": "PesquisarLancamentos",
            "app_key": APP_KEY,
            "app_secret": APP_SECRET,
            "param": [{"nPagina": page, "nRegPorPagina": 100}]
        }
        response = requests.post(
            "https://app.omie.com.br/api/v1/financas/pesquisartitulos/",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body)
        )
        res_json = response.json()
        total_pages = res_json.get("nTotPaginas", 1)
        all_data.extend(res_json.get("titulosEncontrados", []))
        page += 1
        time.sleep(0.3)
    return pd.json_normalize(all_data)

def get_clientes():
    all_data = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        body = {
            "call": "ListarClientes",
            "app_key": APP_KEY,
            "app_secret": APP_SECRET,
            "param": [{
                "pagina": page,
                "registros_por_pagina": 50,
                "apenas_importado_api": "N"
            }]
        }
        response = requests.post(
            "https://app.omie.com.br/api/v1/geral/clientes/",
            headers={"Content-Type": "application/json"},
            data=json.dumps(body)
        )
        res_json = response.json()
        total_pages = res_json.get("total_de_paginas", 1)
        all_data.extend(res_json.get("clientes_cadastro", []))
        page += 1
        time.sleep(0.3)
    return pd.json_normalize(all_data)

# ▶ Baixar dados
print("Baixando títulos...")
df_titulos = get_titulos()
print("Baixando clientes...")
df_clientes = get_clientes()

# ▶ Mesclar e limpar
df_final = pd.merge(
    df_titulos,
    df_clientes,
    left_on='cabecTitulo.nCodCliente',
    right_on='codigo_cliente_omie',
    how='left'
)
df_final["cabecTitulo.cNatureza"] = df_final["cabecTitulo.cNatureza"].replace({
    "P": "Despesa",
    "R": "Receita"
})
df_final = df_final.drop_duplicates(subset=["cabecTitulo.nCodTitulo"])

# ▶ Autenticação
print("Autenticando com Google...")
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
if not json_secret:
    raise ValueError("Variável de ambiente GDRIVE_SERVICE_ACCOUNT não está definida.")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(json_secret)
creds_gspread = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds_gspread)

creds_drive = Credentials.from_service_account_info(creds_dict, scopes=scope)
drive_service = build("drive", "v3", credentials=creds_drive)

# ▶ Procurar planilha na pasta
folder_id = "bc1qf92drq0wwm8w7rnw9d8p4wjvaut2csdd5sg2cx"
sheet_title = "Financeiro_Completo_Pulse"

print("Verificando se a planilha já existe...")
query = f"name = '{sheet_title}' and mimeType = 'application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed = false"
response = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = response.get('files', [])

if files:
    # ▶ Atualizar planilha existente
    file_id = files[0]['id']
    print("Planilha encontrada. Atualizando...")
    spreadsheet = client.open_by_key(file_id)
    worksheet = spreadsheet.get_worksheet(0)
    worksheet.clear()
else:
    # ▶ Criar nova planilha
    print("Planilha não encontrada. Criando nova...")
    spreadsheet = client.create(sheet_title)
    file_id = spreadsheet.id

    # ▶ Mover para a pasta
    previous_parents = drive_service.files().get(fileId=file_id, fields='parents').execute().get('parents', [])
    if previous_parents:
        drive_service.files().update(
            fileId=file_id,
            addParents=folder_id,
            removeParents=",".join(previous_parents),
            fields='id, parents'
        ).execute()
    else:
        drive_service.files().update(
            fileId=file_id,
            addParents=folder_id,
            fields='id, parents'
        ).execute()
    worksheet = spreadsheet.get_worksheet(0)

# ▶ Atualizar conteúdo da planilha
worksheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())

print("Planilha atualizada com sucesso.")
