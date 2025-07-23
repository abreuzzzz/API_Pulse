import requests
import pandas as pd
import json
import time
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# 🔐 Chaves da Omie
APP_KEY = "5519963813364"
APP_SECRET = "756d32d251ef60fb54f21a14c99aa838"  # Substitua pela sua chave real

# Configuração de chamadas API
headers = {"Content-Type": "application/json"}

def get_titulos():
    all_data = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        body = {
            "call": "PesquisarLancamentos",
            "app_key": APP_KEY,
            "app_secret": APP_SECRET,
            "param": [{
                "nPagina": page,
                "nRegPorPagina": 100
            }]
        }

        response = requests.post(
            "https://app.omie.com.br/api/v1/financas/pesquisartitulos/",
            headers=headers,
            data=json.dumps(body)
        )

        res_json = response.json()
        total_pages = res_json.get("nTotPaginas", 1)
        titulos = res_json.get("titulosEncontrados", [])
        all_data.extend(titulos)

        page += 1
        time.sleep(0.3)  # evita throttling

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
            headers=headers,
            data=json.dumps(body)
        )

        res_json = response.json()
        total_pages = res_json.get("total_de_paginas", 1)
        clientes = res_json.get("clientes_cadastro", [])
        all_data.extend(clientes)

        page += 1
        time.sleep(0.3)

    return pd.json_normalize(all_data)

# ▶ Baixar dados
print("Baixando títulos...")
df_titulos = get_titulos()

print("Baixando clientes...")
df_clientes = get_clientes()

# ▶ Mesclar com base em 'nCodCliente' e 'codigo_cliente_omie'
df_final = pd.merge(
    df_titulos,
    df_clientes,
    left_on='cabecTitulo.nCodCliente',
    right_on='codigo_cliente_omie',
    how='left'
)

# ▶ Substituir tipo (P = Despesa, R = Receita)
df_final["cabecTitulo.cNatureza"] = df_final["cabecTitulo.cNatureza"].replace({
    "P": "Despesa",
    "R": "Receita"
})

# ▶ Remover duplicados por código do título
df_final = df_final.drop_duplicates(subset=["cabecTitulo.nCodTitulo"])

# ▶ Exportar para Google Sheets
print("Exportando para Google Sheets...")

# ▶ Autenticação via variável de ambiente (JSON como string)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")

if not json_secret:
    raise ValueError("Variável de ambiente GDRIVE_SERVICE_ACCOUNT não está definida.")

credentials_dict = json.loads(json_secret)
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
client = gspread.authorize(creds)

# ▶ Criar planilha no Google Drive
spreadsheet = client.create("saida_omie")
spreadsheet.share('SEU_EMAIL_GOOGLE@gmail.com', perm_type='user', role='writer')  # Substitua pelo seu e-mail

# ▶ Atualizar planilha
worksheet = spreadsheet.get_worksheet(0)
worksheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())

print("Exportado com sucesso.")
