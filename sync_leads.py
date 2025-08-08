import os
import requests
import re

# --- CONFIGURAÇÕES ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()
RD_STAGE_ID = os.environ.get("RD_STAGE_ID", "").strip()

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- MAPA FINAL E DEFINITIVO (BASEADO NA SUA TABELA) ---
# A chave é o NOME EXATO DA COLUNA NO NOTION.
NOTION_COLUMNS_MAP = {
    # Coluna no Notion      # Fonte no RD Station                     # Tipo no Notion
    'CPF (COMPRADOR)':  {'source': 'custom', 'id_rd': '67b0a46deba3040017bf2c62', 'type': 'text'},
    'Renda':            {'source': 'custom', 'id_rd': '67b31ea6e1f61f0016ce701d', 'type': 'number'},
    'CLT / AUT':        {'source': 'custom', 'id_rd': '67b3210c4ae59b00148582e9', 'type': 'multi_select'},
    'Origem':           {'source': 'standard', 'path': ['deal', 'deal_source', 'name'], 'type': 'select'},
    'Responsável':      {'source': 'standard', 'path': ['deal', 'user', 'name'], 'type': 'multi_select'},
    'Data de Origem':   {'source': 'standard', 'path': ['deal', 'created_at'], 'type': 'date'},
}


# --- FUNÇÕES ---

def create_lead_in_notion(lead_data, normalized_phone, custom_fields_dict):
    """Cria uma página no Notion seguindo o mapeamento exato fornecido."""
    print(f"A criar o lead '{lead_data['name']}' com mapeamento definitivo no Notion...")
    url = "https://api.notion.com/v1/pages"
    
    # Payload base com as propriedades que sempre tentaremos preencher
    properties_payload = {
        "Nome (Completar)": {"title": [{"text": {"content": lead_data.get("name", "Negociação sem nome")}}]},
        "Telefone": {"phone_number": normalized_phone if normalized_phone else None},
    }

    # Itera sobre o mapa para preencher dinamicamente as colunas
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
    print("--- A iniciar o script de sincronização ---")
    rd_leads = fetch_rd_station_leads()
    print(f"Encontradas {len(rd_leads)} negociações na etapa de avaliação.")
    
    for lead in rd_leads:
        lead_name = lead.get('name', 'Nome Desconhecido')
        print(f"\nA processar negociação: {lead_name}")
        
        # Converte a lista de campos personalizados para um dicionário de fácil acesso (ID -> Valor)
        custom_fields_dict = {field.get("custom_field", {}).get("_id"): field.get("value") for field in lead.get("deal_custom_fields", [])}
        
        # Lógica para encontrar o telefone (contato ou campo personalizado)
        lead_phone = ""
        if lead.get("contacts"):
            lead_phone = (lead["contacts"][0].get("phones") or [{}])[0].get("phone")
        if not lead_phone:
            lead_phone = custom_fields_dict.get("67ea8afafddd15001447f639") # ID do campo "ID ou Telefone"

        normalized_phone = normalize_phone_number(lead_phone)
        
        if not normalized_phone:
            print(f"A negociação '{lead_name}' não tem telefone para identificação. A ignorar.")
            continue 
        
        # Se o lead ainda não existe no Notion, cria
        if not find_lead_by_phone(normalized_phone):
            create_lead_in_notion(lead, normalized_phone, custom_fields_dict)
        else:
            print(f"Lead com telefone {normalized_phone} já existe no Notion. A ignorar.")
    
    print("\n--- Script de sincronização finalizado ---")
