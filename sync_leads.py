# --- BIBLIOTECAS ---
import os
import requests
import re
import csv
import datetime
import json
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIGURAÇÕES GERAIS ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()

# --- MAPEAMENTO DE ETAPAS DO RD PARA SITUAÇÕES DO NOTION ---
# CONFIRME SE ESTA LISTA DE ETAPAS ESTÁ COMPLETA
RD_STAGES_MAP = {
    "67ae261cab5a8e00178ea863": "Avaliando",
    "67bcd1b67d60d4001b8c8aa2": "Condicionado",
    "67ae261cab5a8e00178ea864": "Aprovado",
    "67ae261cab5a8e00178ea865": "Com Reserva",
}

# --- NOVO: MAPEAMENTO COMPLETO DE CAMPOS PERSONALIZADOS DO RD PARA O NOTION ---
# "ID do Campo no RD": {"notion_name": "Nome da Coluna no Notion", "notion_type": "tipo"}
NOTION_RD_MAP = {
    "67ea8afafddd15001447f639": {"notion_name": "ID (RD Station)", "notion_type": "text"},
    "67b62f4fad0a4e0014841510": {"notion_name": "De onde é?", "notion_type": "text"},
    "689cf258cece270014dbb4bc": {"notion_name": "Aluguel", "notion_type": "number"},
    "67bdbe2a5062a6001945f18b": {"notion_name": "Por que deseja a casa?", "notion_type": "text"},
    "67b31ac9fce8b4001e8dca11": {"notion_name": "Recebe Bolsa Família", "notion_type": "select"},
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
    # O campo "Aluguel" (689cf258cece270014dbb4bc) está duplicado na sua lista, mantive apenas um.
}

# Configurações do Google Drive
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_CREDENTIALS_JSON = os.environ.get("GDRIVE_CREDENTIALS", "").strip()
GDRIVE_TOKEN_JSON = os.environ.get("GDRIVE_TOKEN_JSON", "").strip()

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- FUNÇÕES AUXILIARES ---

def format_notion_property(value, notion_type):
    """Formata um valor para o tipo de propriedade correto do Notion."""
    if value is None or str(value).strip() == "":
        return None
    try:
        if notion_type == "text":
            return {"rich_text": [{"text": {"content": str(value)}}]}
        elif notion_type == "number":
            # Limpa o valor para aceitar formatos como "R$ 1.234,56"
            cleaned_value = re.sub(r'[^\d,.]', '', str(value))
            numeric_value = float(cleaned_value.replace(",", "."))
            return {"number": numeric_value}
        elif notion_type == "select":
            return {"select": {"name": str(value)}}
        # Adicione outros tipos aqui se necessário (ex: multi_select, date)
    except (ValueError, TypeError) as e:
        print(f"  !! Aviso: Não foi possível formatar o valor '{value}' para o tipo '{notion_type}'. Erro: {e}")
        return None
    return None

def build_properties_payload(lead_data, situacao):
    """Constrói o dicionário de propriedades para a API do Notion."""
    properties = {}
    
    # 1. Campos Padrão e Obrigatórios
    properties["Nome (Completar)"] = {"title": [{"text": {"content": lead_data.get("name", "Negociação sem nome")}}]}
    properties["ID do Lead no RD"] = {"rich_text": [{"text": {"content": lead_data["id"]}}]}
    properties["Situação"] = {"select": {"name": situacao}}
    
    lead_phone = ""
    if lead_data.get("contacts"):
        phones = (lead_data["contacts"][0].get("phones") or [{}])
        if phones: lead_phone = phones[0].get("phone")
    properties["Telefone"] = {"phone_number": normalize_phone_number(lead_phone) if lead_phone else None}

    # 2. Mapeamento de Campos Personalizados
    custom_fields_dict = {field["custom_field"]["_id"]: field["value"] for field in lead_data.get("deal_custom_fields", [])}
    
    for rd_id, notion_info in NOTION_RD_MAP.items():
        rd_value = custom_fields_dict.get(rd_id)
        if rd_value is not None:
            formatted_property = format_notion_property(rd_value, notion_info["notion_type"])
            if formatted_property:
                properties[notion_info["notion_name"]] = formatted_property
                
    return properties

# --- FUNÇÕES DE SINCRONIZAÇÃO ---

def get_existing_notion_leads():
    """Busca todos os leads no Notion e retorna um dicionário mapeando ID do RD para ID da página do Notion."""
    # ... (Esta função continua igual à versão anterior)
    print("A buscar leads existentes no Notion para mapeamento...")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    leads_map = {}
    has_more = True
    next_cursor = None
    
    while has_more:
        payload = {}
        if next_cursor: payload['start_cursor'] = next_cursor
        
        response = requests.post(url, headers=NOTION_HEADERS, json=payload)
        if response.status_code != 200:
            print(f"### ERRO ao buscar leads do Notion: {response.text}")
            return {}
            
        data = response.json()
        for page in data["results"]:
            try:
                rd_id_property = page["properties"].get("ID do Lead no RD", {})
                if rd_id_property and rd_id_property.get("rich_text"):
                    rd_lead_id = rd_id_property["rich_text"][0]["text"]["content"]
                    notion_page_id = page["id"]
                    leads_map[rd_lead_id] = notion_page_id
            except (IndexError, KeyError):
                continue
                
        has_more = data['has_more']
        next_cursor = data['next_cursor']
        
    print(f"Encontrados {len(leads_map)} leads com ID do RD no Notion.")
    return leads_map


def update_lead_in_notion(notion_page_id, lead_data, situacao):
    """ATUALIZADO: Atualiza TODOS os campos de um lead existente no Notion."""
    print(f"  -> A ATUALIZAR lead no Notion (ID do RD: {lead_data['id']})")
    url = f"https://api.notion.com/v1/pages/{notion_page_id}"
    properties_payload = build_properties_payload(lead_data, situacao)
    
    payload = {"properties": properties_payload}
    
    response = requests.patch(url, headers=NOTION_HEADERS, json=payload)
    if response.status_code != 200:
        print(f"  ### ERRO ao atualizar lead no Notion: {response.text}")

def create_lead_in_notion(lead_data, situacao):
    """ATUALIZADO: Cria um novo lead no Notion com TODOS os campos mapeados."""
    print(f"  -> A CRIAR novo lead no Notion: '{lead_data['name']}'")
    url = "https://api.notion.com/v1/pages"
    properties_payload = build_properties_payload(lead_data, situacao)
    
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties_payload
    }
    
    response = requests.post(url, headers=NOTION_HEADERS, json=payload)
    if response.status_code != 200:
        print(f"  ### ERRO ao criar lead no Notion: {response.text}")

def fetch_rd_station_leads_by_stage(stage_id):
    """Busca as negociações de uma etapa específica no RD Station CRM."""
    # ... (Esta função continua igual à versão anterior)
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={stage_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("deals", [])
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar negociações da etapa {stage_id} no RD Station: {e}")
        return []

def normalize_phone_number(phone_str):
    if not phone_str: return ""
    return re.sub(r'\D', '', phone_str)


# --- FLUXO PRINCIPAL DE SINCRONIZAÇÃO ---
if __name__ == "__main__":
    print("\n--- A INICIAR SCRIPT DE SINCRONIZAÇÃO RD -> NOTION (VERSÃO REFINADA) ---")
    
    # 1. Pega o estado atual do Notion
    existing_leads_map = get_existing_notion_leads()
    
    # 2. Itera sobre cada etapa do RD mapeada
    for stage_id, notion_situacao in RD_STAGES_MAP.items():
        print(f"\nA processar etapa do RD: {stage_id} (Situação no Notion: '{notion_situacao}')")
        rd_leads_in_stage = fetch_rd_station_leads_by_stage(stage_id)
        
        if not rd_leads_in_stage:
            print("Nenhum lead encontrado nesta etapa.")
            continue

        print(f"Encontrados {len(rd_leads_in_stage)} leads.")
        
        # 3. Sincroniza cada lead da etapa atual
        for lead in rd_leads_in_stage:
            rd_lead_id = lead["id"]
            
            # --- NOVO: IMPRIME OS DADOS BRUTOS DO LEAD PARA DEPURAÇÃO ---
            print(f"\n--- A processar Lead: {lead.get('name')} (ID RD: {rd_lead_id}) ---")
            # A linha abaixo imprime todos os campos do lead. Útil para depuração.
            # print(json.dumps(lead, indent=2))

            # Verifica se o lead já existe no nosso mapa do Notion
            if rd_lead_id in existing_leads_map:
                notion_page_id = existing_leads_map[rd_lead_id]
                update_lead_in_notion(notion_page_id, lead, notion_situacao)
            else:
                create_lead_in_notion(lead, notion_situacao)

    print("\n--- SCRIPT DE SINCRONIZAÇÃO FINALIZADO ---")
