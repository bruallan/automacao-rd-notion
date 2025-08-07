import os
import requests
import re

# --- CONFIGURAÇÕES ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()
RD_STAGE_ID = os.environ.get("RD_STAGE_ID", "").strip()
BOTCONVERSA_API_KEY = os.environ.get("BOTCONVERSA_API_KEY", "").strip()
WHATSAPP_RECIPIENT_NUMBER = os.environ.get("WHATSAPP_RECIPIENT_NUMBER", "").strip()

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- MAPA DEFINITIVO: Notion -> RD Station ---
# A chave principal é o NOME EXATO DA COLUNA NO NOTION. O script só tentará preencher estas colunas.
NOTION_COLUMNS_MAP = {
    'CPF (COMPRADOR)':    {'id_rd': '67b0a46deba3040017bf2c62', 'type': 'text'},
    'Renda':              {'id_rd': '67b31ea6e1f61f0016ce701d', 'type': 'number'},
    'CLT/AUT':            {'id_rd': '67b3210c4ae59b00148582e9', 'type': 'select'},
    '+3 ANOS DE CLT':     {'id_rd': '67b31f37ca237d001e358c1b', 'type': 'select'},
    'Origem':             {'id_rd': '67b62f4fad0a4e0014841510', 'type': 'select'}, # Mapeando para "DE ONDE É"
    'OBS: entrada? FGTS? FGTS\n Futuro? Limite Cartão?': {'id_rd': '67bdbe2a5062a6001945f18b', 'type': 'text'}, # Mapeando para "POR QUE DESEJA A CASA"
    'Profissão':          {'id_rd': '67b0a3f6b436410018d97957', 'type': 'text'}, # Assumindo que tenha uma coluna 'Profissão'
    'Estado Civil':       {'id_rd': '67b321ba30fafb001c8f8743', 'type': 'select'},
    'Dependente':         {'id_rd': '67b31ed5943c0c001b25cd41', 'type': 'text'}, # Mapeando para "TEM DEPENDENTE"
    # Adicione aqui outros mapeamentos conforme necessário
}


# --- FUNÇÕES ---

def create_lead_in_notion(lead, normalized_phone, custom_fields_dict):
    """Cria uma página no Notion apenas com as colunas que existem e estão mapeadas."""
    print(f"A criar o lead '{lead['name']}' com dados mapeados no Notion...")
    
    url = "https://api.notion.com/v1/pages"
    phone_for_notion = normalized_phone if normalized_phone else None
    
    # Payload base apenas com as propriedades que temos certeza que existem
    properties_payload = {
        "Nome (Completar)": {"title": [{"text": {"content": lead.get("name", "Negociação sem nome")}}]},
        "Telefone": {"phone_number": phone_for_notion},
    }

    # Itera sobre o nosso mapa para preencher dinamicamente apenas as colunas mapeadas
    for notion_col_name, rd_mapping in NOTION_COLUMNS_MAP.items():
        rd_field_id = rd_mapping['id_rd']
        field_value = custom_fields_dict.get(rd_field_id)
        
        if field_value: # Só processa se o campo personalizado tiver algum valor no RD
            notion_type = rd_mapping['type']
            print(f"  - Mapeando campo: '{notion_col_name}' com valor '{field_value}'")

            if notion_type == "text":
                properties_payload[notion_col_name] = {"rich_text": [{"text": {"content": str(field_value)}}]}
            elif notion_type == "select":
                properties_payload[notion_col_name] = {"select": {"name": str(field_value)}}
            elif notion_type == "number":
                try:
                    numeric_value = float(str(field_value).replace(",", "."))
                    properties_payload[notion_col_name] = {"number": numeric_value}
                except (ValueError, TypeError):
                    print(f"  !! Aviso: Não foi possível converter '{field_value}' para número na coluna '{notion_col_name}'.")

    # Monta o payload final e envia para o Notion
    final_payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties_payload}
    
    response = requests.post(url, headers=NOTION_HEADERS, json=final_payload)
    
    if response.status_code == 200:
        print(f"Lead '{lead['name']}' CRIADO COM SUCESSO NO NOTION!")
    else:
        print(f"### ERRO AO CRIAR LEAD NO NOTION ###")
        print(f"Status Code: {response.status_code}")
        print(f"Resposta do Notion: {response.text}")
        print(f"####################################")

# --- FUNÇÕES AUXILIARES (sem alterações) ---
def send_whatsapp_notification(message):
    if not BOTCONVERSA_API_KEY or not WHATSAPP_RECIPIENT_NUMBER:
        print("!! Aviso: Segredos do WhatsApp não configurados. Notificação não enviada.")
        return
    api_url = "https://api.botconversa.com.br/v1/webhooks/send"
    headers = {"Authorization": f"Bearer {BOTCONVERSA_API_KEY}", "Content-Type": "application/json"}
    payload = {"phone": WHATSAPP_RECIPIENT_NUMBER, "type": "text", "value": message}
    try:
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"!! Erro ao enviar notificação para o WhatsApp: {e}")

def normalize_phone_number(phone_str):
    if not phone_str: return ""
    return re.sub(r'\D', '', phone_str)

def find_lead_by_phone(normalized_phone):
    if not normalized_phone: return False
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {"filter": {"property": "Telefone", "phone_number": {"equals": normalized_phone}}}
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)
    if response.status_code == 200 and len(response.json()["results"]) > 0:
        return True
    return False

def fetch_rd_station_leads():
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={RD_STAGE_ID}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("deals", [])
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar negociações no RD Station: {e}")
        return []

# --- FLUXO PRINCIPAL (Atualizado) ---
if __name__ == "__main__":
    print("--- A iniciar o script de sincronização ---")
    rd_leads = fetch_rd_station_leads()
    print(f"Encontradas {len(rd_leads)} negociações na etapa de avaliação.")
    
    if rd_leads:
        for lead in rd_leads:
            lead_name = lead.get('name', 'Nome Desconhecido')
            print(f"\nA processar negociação: {lead_name}")
            
            # Converte a lista de campos personalizados para um dicionário de fácil acesso (ID -> Valor)
            custom_fields_list = lead.get("deal_custom_fields", [])
            custom_fields_dict = {field.get("custom_field", {}).get("_id"): field.get("value") for field in custom_fields_list}
            
            # Lógica para encontrar o telefone
            lead_phone = ""
            contacts_list = lead.get("contacts", [])
            if contacts_list:
                lead_phone = (contacts_list[0].get("phones") or [{}])[0].get("phone")
            
            if not lead_phone:
                id_ou_telefone_id = "67ea8afafddd15001447f639"
                lead_phone = custom_fields_dict.get(id_ou_telefone_id)

            normalized_phone = normalize_phone_number(lead_phone)
            
            if not normalized_phone:
                print(f"A negociação '{lead_name}' não tem telefone. A ignorar.")
                continue 
            
            if not find_lead_by_phone(normalized_phone):
                # Passa o dicionário de campos personalizados para a função de criação
                create_lead_in_notion(lead, normalized_phone, custom_fields_dict)
    
    print("\n--- Script de sincronização finalizado ---")
