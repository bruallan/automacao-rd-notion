import os
import requests
import re

# --- CONFIGURAÇÕES ---
# Segredos do Notion e RD Station
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN")
RD_STAGE_ID = os.environ.get("RD_STAGE_ID")

# Novos segredos para as notificações do WhatsApp
BOTCONVERSA_API_KEY = os.environ.get("BOTCONVERSA_API_KEY")
WHATSAPP_RECIPIENT_NUMBER = os.environ.get("WHATSAPP_RECIPIENT_NUMBER")

# Constantes e Cabeçalhos
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}
CUSTOM_FIELD_NAME_FOR_PHONE = "ID ou telefone"

# --- FUNÇÕES ---

def send_whatsapp_notification(message):
    """Envia uma notificação via API do BotConversa."""
    if not BOTCONVERSA_API_KEY or not WHATSAPP_RECIPIENT_NUMBER:
        print("!! Aviso: Segredos do WhatsApp não configurados. Notificação não enviada.")
        return

    # IMPORTANTE: Verifique a URL e a estrutura do payload na documentação da sua API do BotConversa.
    # Esta é uma estrutura genérica comum.
    api_url = "https://api.botconversa.com.br/v1/webhooks/send" # Exemplo de URL, confirme na sua documentação!
    
    headers = {
        "Authorization": f"Bearer {BOTCONVERSA_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "phone": WHATSAPP_RECIPIENT_NUMBER,
        "type": "text",
        "value": message
    }
    
    try:
        print(f"A enviar notificação para o WhatsApp: '{message}'")
        response = requests.post(api_url, headers=headers, json=payload)
        response.raise_for_status()
        print("Notificação enviada com sucesso.")
    except requests.exceptions.RequestException as e:
        print(f"!! Erro ao enviar notificação para o WhatsApp: {e}")

def normalize_phone_number(phone_str):
    if not phone_str:
        return ""
    return re.sub(r'\D', '', phone_str)

def find_lead_by_phone(normalized_phone):
    # (Função sem alterações)
    if not normalized_phone: return False
    print(f"A verificar no Notion pelo telefone normalizado: '{normalized_phone}'...")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {"filter": {"property": "Telefone", "phone_number": {"equals": normalized_phone}}}
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)
    if response.status_code == 200 and len(response.json()["results"]) > 0:
        print("Lead encontrado. A ignorar.")
        return True
    print("Lead não encontrado.")
    return False

def create_lead_in_notion(lead, normalized_phone):
    # (Função sem alterações)
    print(f"A criar o lead '{lead['name']}' com o telefone '{normalized_phone}' no Notion...")
    url = "https://api.notion.com/v1/pages"
    email = ""
    if lead.get("contacts"): email = (lead["contacts"][0].get("emails") or [{}])[0].get("email", "")
    payload = {"parent": {"database_id": NOTION_DATABASE_ID},"properties": {"Nome do Lead": {"title": [{"text": {"content": lead.get("name", "Negociação sem nome")}}]},"Email": {"email": email},"Telefone": {"phone_number": normalized_phone},"ID do Lead no RD": {"rich_text": [{"text": {"content": str(lead.get("id", ""))}}]}}}
    requests.post(url, headers=NOTION_HEADERS, json=payload)
    print(f"Lead '{lead['name']}' criado com sucesso!")

def fetch_rd_station_leads():
    # (Função sem alterações)
    print("A buscar negociações no RD Station CRM...")
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={RD_STAGE_ID}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        leads = response.json().get("deals", [])
        print(f"Encontradas {len(leads)} negociações na etapa de avaliação.")
        return leads
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar negociações no RD Station: {e}")
        return []

# --- FLUXO PRINCIPAL (COM NOTIFICAÇÕES) ---
if __name__ == "__main__":
    print("--- A iniciar o script de sincronização ---")
    rd_leads = fetch_rd_station_leads()
    
    if rd_leads:
        for lead in rd_leads:
            lead_name = lead.get('name', 'Nome Desconhecido')
            print(f"\nA processar negociação: {lead_name}")
            
            lead_phone = ""
            no_contact_warning_sent = False # Flag para controlar as notificações

            # 1. Tenta pegar o telefone do contato associado
            contacts_list = lead.get("contacts", [])
            if contacts_list:
                contact = contacts_list[0]
                lead_phone = (contact.get("phones") or [{}])[0].get("phone")
                print("Telefone encontrado no contato associado.")
            else:
                # Se não há contato, envia a primeira notificação
                message = f"Atenção: A negociação do RD \"{lead_name}\" está sem contato associado. Tentando buscar telefone em campos personalizados."
                send_whatsapp_notification(message)
                no_contact_warning_sent = True # Ativa a flag

            # 2. Se não encontrou telefone, procura no campo personalizado
            if not lead_phone:
                print("A procurar telefone em campos personalizados...")
                custom_fields = lead.get("deal_custom_fields", [])
                for field in custom_fields:
                    if field.get("custom_field", {}).get("label") == CUSTOM_FIELD_NAME_FOR_PHONE:
                        lead_phone = field.get("value")
                        print("Telefone encontrado no campo personalizado.")
                        break

            # 3. Normaliza e faz a verificação final
            normalized_phone = normalize_phone_number(lead_phone)
            
            if not normalized_phone:
                # Se AINDA não tem telefone, a negociação é inválida para nós
                # E se a primeira notificação já foi enviada, envia a segunda.
                if no_contact_warning_sent:
                    message = f"Falha: A negociação do RD \"{lead_name}\" está sem contato associado E sem telefone no campo personalizado. Nenhuma ação foi tomada."
                    send_whatsapp_notification(message)
                
                print(f"A negociação '{lead_name}' não tem um número de telefone válido em nenhum campo. A ignorar.")
                continue # Pula para o próximo lead
            
            # Se encontrou telefone, prossegue com a lógica de criação
            if not find_lead_by_phone(normalized_phone):
                create_lead_in_notion(lead, normalized_phone)
    
    print("\n--- Script de sincronização finalizado ---")
