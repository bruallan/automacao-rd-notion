# --- BIBLIOTECAS ---
import os
import requests
import re
import csv
import datetime
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIGURAÇÕES ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()
RD_STAGE_ID = os.environ.get("RD_STAGE_ID", "").strip()

# --- NOVO: CONFIGURAÇÕES DO GOOGLE DRIVE ---
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
# As credenciais são lidas da variável de ambiente como uma string JSON
GDRIVE_CREDENTIALS_JSON = os.environ.get("GDRIVE_CREDENTIALS", "").strip()
GDRIVE_TOKEN_JSON = os.environ.get("GDRIVE_TOKEN_JSON", "").strip() # <--- ESTA LINHA PROVAVELMENTE ESTÁ A FALTAR


NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- MAPA FINAL E DEFINITIVO (BASEADO NA SUA TABELA) ---
# ... (o seu mapeamento de colunas continua igual) ...
NOTION_COLUMNS_MAP = {
    # Coluna no Notion      # Fonte no RD Station                     # Tipo no Notion
    'CPF (COMPRADOR)':  {'source': 'custom', 'id_rd': '67b0a46deba3040017bf2c62', 'type': 'text'},
    'Renda':            {'source': 'custom', 'id_rd': '67b31ea6e1f61f0016ce701d', 'type': 'number'},
    'CLT / AUT':        {'source': 'custom', 'id_rd': '67b3210c4ae59b00148582e9', 'type': 'multi_select'},
    'Origem':           {'source': 'standard', 'path': ['deal', 'deal_source', 'name'], 'type': 'select'},
    'Responsável':      {'source': 'standard', 'path': ['deal', 'user', 'name'], 'type': 'multi_select'},
    'Data de Origem':   {'source': 'standard', 'path': ['deal', 'created_at'], 'type': 'date'},
}

# --- NOVO: FUNÇÃO DE UPLOAD PARA O GOOGLE DRIVE ---

def upload_to_google_drive(filename):
    """Faz o upload de um ficheiro para uma pasta específica no Google Drive."""
    print(f"--- A iniciar o upload para o Google Drive: '{filename}' ---")
    try:
        if not GDRIVE_CREDENTIALS_JSON or not GDRIVE_TOKEN_JSON: # Verifica o novo segredo
            print("### ERRO: Credenciais ou token do Google Drive não encontrados...")
            return
        
        # Carrega as credenciais a partir da string JSON
        creds_info = json.loads(GDRIVE_CREDENTIALS_JSON)
        token_info = json.loads(GDRIVE_TOKEN_JSON) # Usa o novo segredo

        # LINHA NOVA E CORRIGIDA
        import google.oauth2.credentials
        creds = google.oauth2.credentials.Credentials.from_authorized_user_info(token_info, scopes=["https://www.googleapis.com/auth/drive"])
                
        service = build('drive', 'v3', credentials=creds)
        
        file_metadata = {
            'name': filename,
            'parents': [GDRIVE_FOLDER_ID]
        }
        
        media = MediaFileUpload(filename, mimetype='text/csv')
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        print(f"✔ Ficheiro carregado com sucesso para o Google Drive! ID do ficheiro: {file.get('id')}")

    except Exception as e:
        print(f"### ERRO AO FAZER UPLOAD PARA O GOOGLE DRIVE: {e} ###")
        print("   Por favor, verifique se a pasta foi partilhada corretamente com o email da conta de serviço.")

# --- FUNÇÃO DE BACKUP ATUALIZADA ---

def extract_property_value(prop):
    """Função auxiliar para extrair o valor de uma propriedade do Notion."""
    prop_type = prop.get('type')
    if not prop_type: return ""
    
    if prop_type in ['title', 'rich_text']:
        return prop[prop_type][0]['text']['content'] if prop.get(prop_type) and prop[prop_type] else ""
    elif prop_type == 'number':
        return prop['number']
    elif prop_type == 'select':
        return prop['select']['name'] if prop.get('select') else ""
    elif prop_type == 'multi_select':
        return ", ".join([item['name'] for item in prop['multi_select']])
    elif prop_type == 'date':
        return prop['date']['start'] if prop.get('date') else ""
    elif prop_type == 'phone_number':
        return prop['phone_number']
    return "N/A"

def backup_notion_database():
    """Busca todos os dados da base do Notion, salva num CSV e faz o upload para o Google Drive."""
    print("--- A iniciar o backup da base de dados do Notion ---")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    all_pages = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {}
        if next_cursor:
            payload['start_cursor'] = next_cursor
            
        try:
            response = requests.post(url, headers=NOTION_HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()
            all_pages.extend(data['results'])
            has_more = data['has_more']
            next_cursor = data['next_cursor']
        except requests.exceptions.RequestException as e:
            print(f"### ERRO AO BUSCAR DADOS DO NOTION PARA BACKUP: {e} ###")
            return

    if not all_pages:
        print("A base de dados do Notion está vazia ou não foi possível aceder. Backup não gerado.")
        return

    processed_data = []
    # MODIFICAÇÃO: Garante que todas as colunas sejam capturadas
    all_headers = set()
    temp_processed = []
    for page in all_pages:
        row = {}
        for prop_name, prop_data in page['properties'].items():
            row[prop_name] = extract_property_value(prop_data)
            all_headers.add(prop_name)
        temp_processed.append(row)

    # Garante que todas as linhas tenham todas as colunas
    header_list = sorted(list(all_headers))
    for row in temp_processed:
        processed_row = {header: row.get(header, "") for header in header_list}
        processed_data.append(processed_row)
        
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"backup_notion_{timestamp}.csv"
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header_list, delimiter=';')
            writer.writeheader()
            writer.writerows(processed_data)
        print(f"Ficheiro de backup temporário '{filename}' criado com sucesso.")
        
        # --- NOVO: Faz o upload do ficheiro ---
        upload_to_google_drive(filename)
        
    except IOError as e:
        print(f"### ERRO AO SALVAR O FICHEIRO DE BACKUP TEMPORÁRIO: {e} ###")
    finally:
        # Apaga o ficheiro local após o upload
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Ficheiro temporário '{filename}' apagado.")


# --- FUNÇÕES EXISTENTES (sem alterações) ---
# ... (create_lead_in_notion, get_value_from_path, etc. continuam aqui) ...
def create_lead_in_notion(lead_data, normalized_phone, custom_fields_dict):
    """Cria uma página no Notion seguindo o mapeamento exato fornecido."""
    print(f"A criar o lead '{lead_data['name']}' com mapeamento definitivo no Notion...")
    url = "https://api.notion.com/v1/pages"
    
    properties_payload = {
        "Nome (Completar)": {"title": [{"text": {"content": lead_data.get("name", "Negociação sem nome")}}]},
        "Telefone": {"phone_number": normalized_phone if normalized_phone else None},
    }

    for notion_col_name, mapping in NOTION_COLUMNS_MAP.items():
        field_value = None
        source_type = mapping['source']

        if source_type == 'custom':
            field_value = custom_fields_dict.get(mapping['id_rd'])
        elif source_type == 'standard':
            field_value = get_value_from_path({'deal': lead_data}, mapping['path'])

        if field_value:
            notion_type = mapping['type']
            print(f"  - Mapeando campo: '{notion_col_name}' com valor '{field_value}' (Tipo: {notion_type})")
            try:
                if notion_type == "text":
                    properties_payload[notion_col_name] = {"rich_text": [{"text": {"content": str(field_value)}}]}
                elif notion_type == "select":
                    properties_payload[notion_col_name] = {"select": {"name": str(field_value)}}
                elif notion_type == "multi_select":
                    properties_payload[notion_col_name] = {"multi_select": [{"name": str(field_value)}]}
                elif notion_type == "number":
                    cleaned_value = re.sub(r'[^\d,.]', '', str(field_value))
                    numeric_value = float(cleaned_value.replace(",", "."))
                    properties_payload[notion_col_name] = {"number": numeric_value}
                elif notion_type == "date":
                    date_only = str(field_value).split('T')[0]
                    properties_payload[notion_col_name] = {"date": {"start": date_only}}
            except Exception as e:
                print(f"  !! Aviso: Falha ao formatar o valor '{field_value}' para a coluna '{notion_col_name}'. Erro: {e}")

    final_payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties_payload}
    
    response = requests.post(url, headers=NOTION_HEADERS, json=final_payload)
    
    if response.status_code == 200:
        print(f"Lead '{lead_data['name']}' CRIADO COM SUCESSO NO NOTION!")
    else:
        print(f"### ERRO AO CRIAR LEAD NO NOTION ###")
        print(f"Status Code: {response.status_code}")
        print(f"Resposta do Notion: {response.text}")
        print(f"####################################")

def get_value_from_path(data_dict, path):
    """Navega num dicionário aninhado para buscar um valor."""
    for key in path:
        data_dict = data_dict.get(key)
        if data_dict is None: return None
    return data_dict

def normalize_phone_number(phone_str):
    if not phone_str: return ""
    return re.sub(r'\D', '', phone_str)

def find_lead_by_phone(normalized_phone):
    """Verifica se um lead com um telefone específico já existe para evitar duplicados."""
    if not normalized_phone: return False
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {"filter": {"property": "Telefone", "phone_number": {"equals": normalized_phone}}}
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)
    return response.status_code == 200 and len(response.json()["results"]) > 0

def fetch_rd_station_leads():
    """Busca as negociações da etapa correta no RD Station CRM."""
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={RD_STAGE_ID}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("deals", [])
    except requests.exceptions.RequestException:
        print(f"Erro ao buscar negociações no RD Station.")
        return []

# --- FLUXO PRINCIPAL ---
if __name__ == "__main__":
    # A função de backup agora salva o CSV e faz o upload para o Drive
    backup_notion_database()
    
    print("\n--- A iniciar o script de sincronização ---")
    rd_leads = fetch_rd_station_leads()
    print(f"Encontradas {len(rd_leads)} negociações na etapa de avaliação.")
    
    for lead in rd_leads:
        lead_name = lead.get('name', 'Nome Desconhecido')
        print(f"\nA processar negociação: {lead_name}")
        
        custom_fields_dict = {field.get("custom_field", {}).get("_id"): field.get("value") for field in lead.get("deal_custom_fields", [])}
        
        lead_phone = ""
        if lead.get("contacts"):
            lead_phone = (lead["contacts"][0].get("phones") or [{}])[0].get("phone")
        if not lead_phone:
            lead_phone = custom_fields_dict.get("67ea8afafddd15001447f639")

        normalized_phone = normalize_phone_number(lead_phone)
        
        if not normalized_phone:
            print(f"A negociação '{lead_name}' não tem telefone para identificação. A ignorar.")
            continue
        
        if not find_lead_by_phone(normalized_phone):
            create_lead_in_notion(lead, normalized_phone, custom_fields_dict)
        else:
            print(f"Lead com telefone {normalized_phone} já existe no Notion. A ignorar.")
    
    print("\n--- Script de sincronização finalizado ---")
