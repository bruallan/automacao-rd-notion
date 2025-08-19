# --- BIBLIOTECAS ---
import os
import requests
import re
import json

# --- CONFIGURAÇÕES GERAIS ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()

# --- NOVO: CONFIGURAÇÕES DO WHATSAPP ---
BOTCONVERSA_API_KEY = os.environ.get("BOTCONVERSA_API_KEY", "").strip()
WHATSAPP_RECIPIENT_NUMBER_BRUNO = os.environ.get("WHATSAPP_RECIPIENT_NUMBER", "").strip()

# --- MAPEAMENTO DE ETAPAS DO RD PARA SITUAÇÕES DO NOTION ---
RD_STAGES_MAP = {
    "67ae261cab5a8e00178ea863": "Avaliando",
    "67bcd1b67d60d4001b8c8aa2": "Condicionado",
    "67ae261cab5a8e00178ea864": "Aprovado",
    "67ae261cab5a8e00178ea865": "Com Reserva",
}

# --- MAPEAMENTO COMPLETO DE CAMPOS PERSONALIZADOS DO RD PARA O NOTION ---
NOTION_RD_MAP = {
    # ... (o seu mapa de campos continua o mesmo)
    "67ea8afafddd15001447f639": {"notion_name": "ID (RD Station)", "notion_type": "text"},
    "67b62f4fad0a4e0014841510": {"notion_name": "De onde é?", "notion_type": "text"},
    "689cf258cece270014dbb4bc": {"notion_name": "Aluguel", "notion_type": "number"},
    "67bdbe2a5062a6001945f18b": {"notion_name": "Por que deseja a casa?", "notion_type": "text"},
    "67b31ac9fce8b4001e8dca11": {"notion_name": "Recebe Bolsa Família?", "notion_type": "select"},
    "67b0a3f6b436410018d97957": {"notion_name": "Profissão", "notion_type": "text"},
    "67b31e93786c3f00143b07ce": {"notion_name": "Idade", "notion_type": "number"},
    "67b321ba30fafb001c8f8743": {"notion_name": "Estado Civil", "notion_type": "text"},
    "67b5d8552b873a001c9cca66": {"notion_name": "Dependente", "notion_type": "select"},
    "67b31f37ca237d001e358c1b": {"notion_name": "+3 anos CLT", "notion_type": "select"},
    "680cadbefcff56001b6be1a8": {"notion_name": "CPF (COOBRIGADO)", "notion_type": "text"},
    "67c9dfefcbf7520014b42750": {"notion_name": "Faixa de Valor da Dívida", "notion_type": "select"},
    "689b40f4249be2001b75ca0c": {"notion_name": "Gênero", "notion_type": "select"},
    "689b4185efda16001986bcfb": {"notion_name": "Local de Trabalho", "notion_type": "text"},
    "689ceff78b78010021d0c5c5": {"notion_name": "Prestação Máxima", "notion_type": "number"},
    "689cf00a43244c00142f8783": {"notion_name": "Parcela Aprovada", "notion_type": "number"},
    "689cf024a5042d0014cd3b3e": {"notion_name": "Entrada Aprovada", "notion_type": "text"},
    "689cf0370f0eb500193694da": {"notion_name": "Saldo FGTS", "notion_type": "text"},
    "689cf0578c1400001473b22e": {"notion_name": "Subsídio Real", "notion_type": "number"},
    "689cf22fb742ff0014c8ba3b": {"notion_name": "OBS: entrada? FGTS? FGTS Futuro? Limite Cartão?", "notion_type": "text"},
}

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- NOVO: FUNÇÃO DE INTEGRAÇÃO COM WHATSAPP ---

def send_whatsapp_message(message):
    """Envia uma mensagem de texto para o WhatsApp via BotConversa."""
    if not BOTCONVERSA_API_KEY or not WHATSAPP_RECIPIENT_NUMBER_BRUNO:
        print("!! Aviso: API Key do BotConversa ou número do destinatário não configurados. Mensagem não enviada.")
        return

    url = "https://backend.botconversa.com.br/api/v1/webhook/send/"
    headers = {
        "Content-Type": "application/json",
        "API-KEY": BOTCONVERSA_API_KEY
    }
    payload = {
        "type": "text",
        "phone": WHATSAPP_RECIPIENT_NUMBER_BRUNO,
        "message": message
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print("   - Mensagem de notificação enviada para o WhatsApp.")
    except requests.exceptions.RequestException as e:
        print(f"   ### ERRO ao enviar mensagem para o WhatsApp: {e}")

# --- FUNÇÕES AUXILIARES E DE SINCRONIZAÇÃO (ATUALIZADAS) ---

def format_notion_property(value, notion_type):
    # ... (Função de formatação continua a mesma)
    if value is None or str(value).strip() == "": return None
    try:
        if notion_type == "text": return {"rich_text": [{"text": {"content": str(value)}}]}
        elif notion_type == "number":
            s_value = str(value).replace("R$", "").strip().replace(".", "").replace(",", ".")
            cleaned_value = re.sub(r'[^\d.]', '', s_value)
            if cleaned_value: return {"number": float(cleaned_value)}
        elif notion_type == "select": return {"select": {"name": str(value)}}
    except (ValueError, TypeError) as e:
        print(f"  !! Aviso: Não foi possível formatar o valor '{value}' para o tipo '{notion_type}'. Erro: {e}")
        return None
    return None

def build_properties_payload(lead_data, situacao):
    # ... (Função de construção de payload continua a mesma)
    properties = {}
    properties["Nome (Completar)"] = {"title": [{"text": {"content": lead_data.get("name", "Negociação sem nome")}}]}
    properties["ID (RD Station)"] = {"rich_text": [{"text": {"content": lead_data["id"]}}]}
    properties["Status"] = {"multi_select": [{"name": situacao}]}
    lead_phone = ""
    if lead_data.get("contacts"):
        phones = (lead_data["contacts"][0].get("phones") or [{}])
        if phones: lead_phone = phones[0].get("phone")
    properties["Telefone"] = {"phone_number": normalize_phone_number(lead_phone) if lead_phone else None}
    custom_fields_dict = {field["custom_field"]["_id"]: field["value"] for field in lead_data.get("deal_custom_fields", [])}
    for rd_id, notion_info in NOTION_RD_MAP.items():
        rd_value = custom_fields_dict.get(rd_id)
        if rd_value is not None:
            formatted_property = format_notion_property(rd_value, notion_info["notion_type"])
            if formatted_property:
                properties[notion_info["notion_name"]] = formatted_property
    return properties

def get_existing_notion_leads():
    """ATUALIZADO: Busca leads e retorna dois dicionários, um por ID do RD e outro por Telefone."""
    print("A buscar leads existentes no Notion para mapeamento...")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    rd_id_map, phone_map = {}, {}
    has_more, next_cursor = True, None
    
    while has_more:
        payload = {}
        if next_cursor: payload['start_cursor'] = next_cursor
        response = requests.post(url, headers=NOTION_HEADERS, json=payload)
        if response.status_code != 200:
            print(f"### ERRO ao buscar leads do Notion: {response.text}"); return {}, {}
        data = response.json()
        
        for page in data["results"]:
            page_id = page["id"]
            props = page["properties"]
            current_status = (props.get("Status", {}).get("multi_select") or [{}])[0].get("name")
            
            try:
                rd_id_prop = props.get("ID (RD Station)", {})
                if rd_id_prop.get("rich_text"):
                    rd_id = rd_id_prop["rich_text"][0]["text"]["content"]
                    if rd_id: rd_id_map[rd_id] = {"page_id": page_id, "status": current_status}
            except (IndexError, KeyError): pass
            
            try:
                phone_prop = props.get("Telefone", {})
                if phone_prop.get("phone_number"):
                    phone = normalize_phone_number(phone_prop["phone_number"])
                    if phone: phone_map[phone] = {"page_id": page_id, "status": current_status}
            except (IndexError, KeyError): pass
                
        has_more, next_cursor = data['has_more'], data['next_cursor']
        
    print(f"Encontrados {len(rd_id_map)} leads com ID do RD e {len(phone_map)} leads com telefone no Notion.")
    return rd_id_map, phone_map

def update_lead_in_notion(page_info, lead_data, situacao):
    """ATUALIZADO: Implementa a lógica híbrida de atualização."""
    rd_lead_id = lead_data["id"]
    lead_name = lead_data.get("name", "Nome Desconhecido")
    print(f"  -> A ATUALIZAR lead no Notion: '{lead_name}' (ID do RD: {rd_lead_id})")
    
    url = f"https://api.notion.com/v1/pages/{page_info['page_id']}"
    properties_payload = build_properties_payload(lead_data, situacao)
    
    # Lógica de exceção para o Status
    current_status = page_info.get("status")
    if current_status and current_status != situacao:
        print(f"  !! Aviso: Status no Notion ('{current_status}') é diferente do esperado ('{situacao}'). O Status não será alterado.")
        del properties_payload["Status"] # Remove o status do payload para não o alterar
        # Envia notificação no WhatsApp sobre a divergência
        whatsapp_alert = (
            f"⚠️ *Alerta de Sincronização*\n\n"
            f"O lead *{lead_name}* foi atualizado, mas o status não foi alterado.\n\n"
            f"*- Status no Notion:* {current_status}\n"
            f"*- Etapa no RD Station deveria ser:* {situacao}\n\n"
            f"Por favor, verifique se a divergência é intencional."
        )
        send_whatsapp_message(whatsapp_alert)

    payload = {"properties": properties_payload}
    response = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    if response.status_code == 200: return f"- Lead atualizado: *{lead_name}*\n  (Status: {current_status or situacao})"
    else: print(f"  ### ERRO ao atualizar lead no Notion: {response.text}"); return None

def create_lead_in_notion(lead_data, situacao):
    """ATUALIZADO: Cria um novo lead e retorna um resumo para o WhatsApp."""
    lead_name = lead_data.get("name", "Negociação sem nome")
    print(f"  -> A CRIAR novo lead no Notion: '{lead_name}'")
    url = "https://api.notion.com/v1/pages"
    properties_payload = build_properties_payload(lead_data, situacao)
    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties_payload}
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)

    if response.status_code == 200:
        lead_phone = properties_payload.get("Telefone", {}).get("phone_number", "N/A")
        return (f"*Lead Adicionado*\n"
                f"- *Nome:* {lead_name}\n"
                f"- *Telefone:* {lead_phone}\n"
                f"- *Status:* {situacao}")
    else:
        print(f"  ### ERRO ao criar lead no Notion: {response.text}")
        return None

def fetch_rd_station_leads_by_stage(stage_id):
    # ... (Função continua a mesma)
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={stage_id}"
    try: response = requests.get(url); response.raise_for_status(); return response.json().get("deals", [])
    except requests.exceptions.RequestException as e: print(f"Erro ao buscar negociações da etapa {stage_id} no RD Station: {e}"); return []

def normalize_phone_number(phone_str):
    if not phone_str: return ""; return re.sub(r'\D', '', phone_str)

# --- FLUXO PRINCIPAL DE SINCRONIZAÇÃO ---
if __name__ == "__main__":
    print("\n--- A INICIAR SCRIPT DE SINCRONIZAÇÃO RD -> NOTION (VERSÃO FINAL) ---")
    
    created_leads_summary = []
    updated_leads_summary = []
    
    rd_id_map, phone_map = get_existing_notion_leads()
    
    for stage_id, notion_situacao in RD_STAGES_MAP.items():
        print(f"\nA processar etapa do RD: {stage_id} (Situação no Notion: '{notion_situacao}')")
        rd_leads_in_stage = fetch_rd_station_leads_by_stage(stage_id)
        if not rd_leads_in_stage: print("Nenhum lead encontrado nesta etapa."); continue
        print(f"Encontrados {len(rd_leads_in_stage)} leads.")
        
        for lead in rd_leads_in_stage:
            rd_lead_id = lead["id"]
            lead_phone = ""
            if lead.get("contacts"):
                phones = (lead["contacts"][0].get("phones") or [{}])
                if phones: lead_phone = phones[0].get("phone")
            normalized_phone = normalize_phone_number(lead_phone)
            
            print(f"\n--- A processar Lead: {lead.get('name')} (ID RD: {rd_lead_id}) ---")
            
            page_info = rd_id_map.get(rd_lead_id)
            if not page_info and normalized_phone:
                page_info = phone_map.get(normalized_phone)

            if page_info:
                summary = update_lead_in_notion(page_info, lead, notion_situacao)
                if summary: updated_leads_summary.append(summary)
            else:
                summary = create_lead_in_notion(lead, notion_situacao)
                if summary: created_leads_summary.append(summary)

    # --- NOVO: Envio do Relatório Final para o WhatsApp ---
    print("\n--- A preparar o relatório final da sincronização ---")
    final_report = ""
    if created_leads_summary:
        final_report += "✅ *Novos Leads Adicionados ao Notion*\n\n" + "\n\n".join(created_leads_summary)
    if updated_leads_summary:
        if final_report: final_report += "\n\n---\n\n"
        final_report += "🔄 *Leads Existentes que Foram Atualizados*\n\n" + "\n".join(updated_leads_summary)
    
    if final_report:
        send_whatsapp_message(final_report)
    else:
        print("Nenhuma alteração foi realizada. Nenhum relatório a ser enviado.")

    print("\n--- SCRIPT DE SINCRONIZAÇÃO FINALIZADO ---")
