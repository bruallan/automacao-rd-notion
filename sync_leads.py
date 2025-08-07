import os
import requests
import json

# --- CONFIGURAÇÕES ---
# Pega as chaves de API dos segredos do GitHub Actions
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN")
RD_STAGE_ID = os.environ.get("RD_STAGE_ID")

# Cabeçalhos padrão para as chamadas da API do Notion
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- FUNÇÕES ---

def find_lead_in_notion(rd_lead_id):
    """Verifica se um lead com um ID específico do RD Station já existe no Notion."""
    print(f"A verificar se o lead com ID do RD '{rd_lead_id}' já existe no Notion...")
    
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    
    # Monta o filtro para procurar pelo ID do Lead no RD
    query_payload = {
        "filter": {
            "property": "ID do Lead no RD", # Nome exato da coluna no Notion
            "rich_text": {
                "equals": str(rd_lead_id)
            }
        }
    }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=query_payload)
    
    if response.status_code == 200:
        data = response.json()
        if len(data["results"]) > 0:
            print("Lead encontrado.")
            return True # O lead já existe
    else:
        print(f"Erro ao procurar no Notion: {response.text}")
        
    print("Lead não encontrado.")
    return False # O lead não existe

def create_lead_in_notion(lead):
    """Cria uma nova página (linha) no Notion com os dados do lead."""
    print(f"A criar o lead '{lead['name']}' no Notion...")
    
    url = "https://api.notion.com/v1/pages"
    
    contact = lead.get("contacts", [{}])[0]
    email = (contact.get("emails") or [{}])[0].get("email", "")
    phone = (contact.get("phones") or [{}])[0].get("phone", "")
    
    create_payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "Nome do Lead": {"title": [{"text": {"content": lead.get("name", "Negociação sem nome")}}]},
            "Email": {"email": email},
            "Telefone": {"phone_number": phone},
            "ID do Lead no RD": {"rich_text": [{"text": {"content": str(lead["id"])}}]}
        }
    }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=create_payload)
    
    if response.status_code == 200:
        print(f"Lead '{lead['name']}' criado com sucesso no Notion!")
    else:
        print(f"Falha ao criar lead no Notion: {response.status_code} - {response.text}")

def fetch_rd_station_leads():
    """Busca todas as negociações da etapa especificada no RD Station CRM."""
    print("A buscar negociações no RD Station CRM...")
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={RD_STAGE_ID}"
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Lança um erro para respostas HTTP > 400
        data = response.json()
        leads = data.get("deals", [])
        print(f"Encontradas {len(leads)} negociações na etapa de avaliação.")
        return leads
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar negociações no RD Station: {e}")
        return []

# --- FLUXO PRINCIPAL ---
if __name__ == "__main__":
    print("--- A iniciar o script de sincronização RD Station -> Notion ---")
    rd_leads = fetch_rd_station_leads()
    
    if rd_leads:
        for lead in rd_leads:
            lead_id = lead["id"]
            if not find_lead_in_notion(lead_id):
                create_lead_in_notion(lead)
    
    print("--- Script de sincronização finalizado ---")
