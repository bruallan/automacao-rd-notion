import os
import requests
import re # Importamos a biblioteca de expressões regulares para a limpeza do telefone

# --- CONFIGURAÇÕES ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN")
RD_STAGE_ID = os.environ.get("RD_STAGE_ID")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

CUSTOM_FIELD_NAME_FOR_PHONE = "ID ou telefone" # Nome do campo personalizado a ser procurado

# --- FUNÇÕES ---

def normalize_phone_number(phone_str):
    """
    Remove todos os caracteres não numéricos de uma string de telefone.
    Ex: "+55 (79) 99999-8888" -> "5579999998888"
    """
    if not phone_str:
        return ""
    return re.sub(r'\D', '', phone_str)

def find_lead_by_phone(normalized_phone):
    """Verifica se um lead com um telefone NORMALIZADO já existe no Notion."""
    if not normalized_phone:
        return False
        
    print(f"A verificar no Notion pelo telefone normalizado: '{normalized_phone}'...")
    
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    
    query_payload = {
        "filter": {
            "property": "Telefone",
            "phone_number": {
                "equals": normalized_phone
            }
        }
    }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=query_payload)
    
    if response.status_code == 200:
        data = response.json()
        if len(data["results"]) > 0:
            print("Lead encontrado. A ignorar.")
            return True
    else:
        print(f"Erro ao procurar no Notion: {response.text}")
        
    print("Lead não encontrado.")
    return False

def create_lead_in_notion(lead, normalized_phone):
    """Cria uma nova página no Notion com os dados e o telefone NORMALIZADO."""
    print(f"A criar o lead '{lead['name']}' com o telefone '{normalized_phone}' no Notion...")
    
    url = "https://api.notion.com/v1/pages"
    
    contact = lead.get("contacts", [{}])[0]
    email = (contact.get("emails") or [{}])[0].get("email", "")
    
    create_payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "Nome do Lead": {"title": [{"text": {"content": lead.get("name", "Negociação sem nome")}}]},
            "Email": {"email": email},
            "Telefone": {"phone_number": normalized_phone}, # Salva o número já limpo
            "ID do Lead no RD": {"rich_text": [{"text": {"content": str(lead.get("id", ""))}}]}
        }
    }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=create_payload)
    
    if response.status_code == 200:
        print(f"Lead '{lead['name']}' criado com sucesso!")
    else:
        print(f"Falha ao criar lead: {response.status_code} - {response.text}")

def fetch_rd_station_leads():
    """Busca negociações da etapa especificada no RD Station."""
    print("A buscar negociações no RD Station CRM...")
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={RD_STAGE_ID}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        leads = data.get("deals", [])
        print(f"Encontradas {len(leads)} negociações na etapa de avaliação.")
        return leads
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar negociações no RD Station: {e}")
        return []

# --- FLUXO PRINCIPAL (ATUALIZADO) ---
if __name__ == "__main__":
    print("--- A iniciar o script de sincronização ---")
    rd_leads = fetch_rd_station_leads()
    
    if rd_leads:
        for lead in rd_leads:
            print(f"\nA processar negociação: {lead.get('name')}")
            
            lead_phone = ""
            
            # 1. Tenta pegar o telefone do contato associado
            contact = lead.get("contacts", [{}])[0]
            if contact:
                lead_phone = (contact.get("phones") or [{}])[0].get("phone")

            # 2. Se não encontrou, tenta pegar do campo personalizado da negociação
            if not lead_phone:
                print("Telefone não encontrado no contato. A procurar em campos personalizados...")
                custom_fields = lead.get("deal_custom_fields", [])
                for field in custom_fields:
                    if field.get("custom_field", {}).get("label") == CUSTOM_FIELD_NAME_FOR_PHONE:
                        lead_phone = field.get("value")
                        break # Para a busca assim que encontrar

            # Normaliza o telefone encontrado
            normalized_phone = normalize_phone_number(lead_phone)
            
            if not normalized_phone:
                print(f"A negociação '{lead.get('name')}' não tem um número de telefone válido. A ignorar.")
                continue
            
            # Verifica a existência pelo telefone NORMALIZADO antes de criar
            if not find_lead_by_phone(normalized_phone):
                create_lead_in_notion(lead, normalized_phone)
    
    print("\n--- Script de sincronização finalizado ---")
