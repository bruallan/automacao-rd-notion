# --- BIBLIOTECAS ---
import os
import requests
import re
import csv
import datetime
import json
import time
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIGURAÇÕES GERAIS ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()

# --- CONFIGURAÇÕES DO WHATSAPP ---
BOTCONVERSA_API_KEY = os.environ.get("BOTCONVERSA_API_KEY", "").strip()
WHATSAPP_RECIPIENT_NUMBER = os.environ.get("WHATSAPP_RECIPIENT_NUMBER", "").strip()
BOTCONVERSA_BASE_URL = "https://backend.botconversa.com.br" 

# --- CONFIGURAÇÕES DO GOOGLE DRIVE ---
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_CREDENTIALS_JSON = os.environ.get("GDRIVE_CREDENTIALS_JSON", "").strip()
GDRIVE_TOKEN_JSON = os.environ.get("GDRIVE_TOKEN_JSON", "").strip()

# --- MAPEAMENTOS ---
RD_STAGES_MAP = {
    "67ae261cab5a8e00178ea863": "Avaliando",
    "67bcd1b67d60d4001b8c8aa2": "Condicionado",
    "67ae261cab5a8e00178ea864": "Aprovado",
    "67ae261cab5a8e00178ea865": "Com Reserva",
}
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# --- FUNÇÕES DE BACKUP E UPLOAD ---
# (Estas funções não foram alteradas)
def upload_to_google_drive(filename):
    print(f"--- A iniciar o upload do backup para o Google Drive: '{filename}' ---")
    try:
        if not GDRIVE_CREDENTIALS_JSON or not GDRIVE_TOKEN_JSON:
            print("### ERRO: Credenciais ou token do Google Drive não encontrados. Verifique os GitHub Secrets. ###"); return
        creds_info = json.loads(GDRIVE_CREDENTIALS_JSON)
        token_info = json.loads(GDRIVE_TOKEN_JSON)
        creds = google.oauth2.credentials.Credentials.from_authorized_user_info(token_info, scopes=["https://www.googleapis.com/auth/drive"])
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': filename, 'parents': [GDRIVE_FOLDER_ID]}
        media = MediaFileUpload(filename, mimetype='text/csv')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✔ Backup carregado com sucesso para o Google Drive! ID do ficheiro: {file.get('id')}")
    except Exception as e:
        print(f"### ERRO AO FAZER UPLOAD DO BACKUP PARA O GOOGLE DRIVE: {e} ###")

def extract_backup_property_value(prop):
    prop_type = prop.get('type')
    if not prop_type: return ""
    if prop_type in ['title', 'rich_text']:
        return prop[prop_type][0]['text']['content'] if prop.get(prop_type) and prop[prop_type] else ""
    elif prop_type == 'number': return prop['number']
    elif prop_type == 'select': return prop['select']['name'] if prop.get('select') else ""
    elif prop_type == 'multi_select': return ", ".join([item['name'] for item in prop['multi_select']])
    elif prop_type == 'date': return prop['date']['start'] if prop.get('date') else ""
    elif prop_type == 'phone_number': return prop['phone_number']
    return "N/A"

def backup_notion_database():
    print("--- A iniciar o backup da base de dados do Notion ---")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    all_pages = []
    has_more, next_cursor = True, None
    while has_more:
        payload = {}
        if next_cursor: payload['start_cursor'] = next_cursor
        try:
            response = requests.post(url, headers=NOTION_HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()
            all_pages.extend(data['results'])
            has_more, next_cursor = data['has_more'], data['next_cursor']
        except requests.exceptions.RequestException as e:
            print(f"### ERRO AO BUSCAR DADOS DO NOTION PARA BACKUP: {e} ###"); return
    if not all_pages:
        print("A base de dados do Notion está vazia. Backup não gerado."); return
    processed_data, all_headers = [], set()
    temp_processed = []
    for page in all_pages:
        row = {}
        for prop_name, prop_data in page['properties'].items():
            row[prop_name] = extract_backup_property_value(prop_data)
            all_headers.add(prop_name)
        temp_processed.append(row)
    header_list = sorted(list(all_headers))
    for row in temp_processed:
        processed_data.append({header: row.get(header, "") for header in header_list})
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"backup_notion_{timestamp}.csv"
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header_list, delimiter=';')
            writer.writeheader()
            writer.writerows(processed_data)
        print(f"Ficheiro de backup temporário '{filename}' criado com sucesso.")
        upload_to_google_drive(filename)
    except IOError as e:
        print(f"### ERRO AO SALVAR O FICHEIRO DE BACKUP TEMPORÁRIO: {e} ###")
    finally:
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Ficheiro temporário '{filename}' apagado.")

# --- FUNÇÕES DE WHATSAPP (REESCRITAS COM A LÓGICA VALIDADA) ---

def _get_botconversa_phone_map():
    """
    Busca todos os subscritores do BotConversa e cria um mapa de 'telefone -> id'.
    Esta lógica é uma adaptação da sua função `processar_contatos_botconversa`.
    """
    print("   - A buscar e mapear contatos do BotConversa...")
    mapa_telefone_id = {}
    url = f"{BOTCONVERSA_BASE_URL}/webhook/subscribers/"
    headers = {"API-KEY": BOTCONVERSA_API_KEY}
    
    pagina = 1
    while url:
        try:
            print(f"   -> Lendo página {pagina} de contatos...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            dados = response.json()

            for inscrito in dados.get('results', []):
                telefone_normalizado = re.sub(r'\D', '', str(inscrito.get('phone')))
                subscriber_id = inscrito.get('id')
                if telefone_normalizado and subscriber_id:
                    mapa_telefone_id[telefone_normalizado] = subscriber_id
            
            url = dados.get('next')
            pagina += 1
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"   -> FALHA ao processar contatos do BotConversa: {e}")
            return None # Retorna None em caso de falha

    print(f"   - Mapeamento concluído. {len(mapa_telefone_id)} contatos carregados.")
    return mapa_telefone_id

def send_whatsapp_message(message):
    """Encontra o ID do subscritor e envia a mensagem para o WhatsApp."""
    if not BOTCONVERSA_API_KEY or not WHATSAPP_RECIPIENT_NUMBER:
        print("!! Aviso: API Key ou número do destinatário não configurados. Mensagem não enviada.")
        return

    mapa_telefone_id = _get_botconversa_phone_map()
    if mapa_telefone_id is None:
        print("   -> Envio de mensagem cancelado devido a uma falha ao buscar contatos.")
        return

    subscriber_id = mapa_telefone_id.get(WHATSAPP_RECIPIENT_NUMBER)
    if not subscriber_id:
        print(f"   -> ERRO: O seu número {WHATSAPP_RECIPIENT_NUMBER} não foi encontrado na lista de subscritores do BotConversa.")
        return

    # Usando o endpoint e o payload validados pelo seu script
    url = f"{BOTCONVERSA_BASE_URL}/webhook/subscriber/{subscriber_id}/send_message/"
    headers = {"Content-Type": "application/json", "API-KEY": BOTCONVERSA_API_KEY}
    payload = {"type": "text", "value": message}
    
    try:
        print(f"   -> A enviar mensagem para o subscritor ID: {subscriber_id}")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        print("   - Mensagem de resumo enviada com sucesso para o WhatsApp.")
    except requests.exceptions.RequestException as e:
        print(f"   ### ERRO ao enviar mensagem para o WhatsApp: {e}")


# --- FUNÇÕES DE SINCRONIZAÇÃO (RESTANTES) ---
# (Não foram alteradas)
def get_existing_notion_leads():
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
            page_id, props = page["id"], page["properties"]
            current_status_list = props.get("Status", {}).get("multi_select", [])
            current_status = current_status_list[0]["name"] if current_status_list else None
            try:
                rd_id_prop = props.get("ID (RD Station)", {})
                id_field_list = rd_id_prop.get("rich_text")
                if id_field_list:
                    rd_id = id_field_list[0]["text"]["content"]
                    if rd_id: rd_id_map[rd_id] = {"page_id": page_id, "status": current_status}
            except (IndexError, KeyError): pass
            try:
                phone_prop = props.get("Telefone", {})
                if phone_prop.get("phone_number"):
                    phone = re.sub(r'\D', '', str(phone_prop["phone_number"]))
                    if phone: phone_map[phone] = {"page_id": page_id, "status": current_status}
            except (IndexError, KeyError): pass
        has_more, next_cursor = data['has_more'], data['next_cursor']
    print(f"Encontrados {len(rd_id_map)} leads com ID do RD e {len(phone_map)} leads com telefone no Notion.")
    return rd_id_map, phone_map

def fetch_rd_station_leads_by_stage(stage_id):
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={stage_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("deals", [])
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar negociações da etapa {stage_id} no RD Station: {e}"); return []

# --- FLUXO PRINCIPAL (MODO DE SIMULAÇÃO) ---
if __name__ == "__main__":
    
    backup_notion_database()

    print("\n--- A INICIAR SIMULAÇÃO DE SINCRONIZAÇÃO (nenhuma alteração será feita no Notion) ---")
    leads_a_criar, leads_a_atualizar = [], []
    rd_id_map, phone_map = get_existing_notion_leads()
    for stage_id, notion_situacao in RD_STAGES_MAP.items():
        print(f"\nA analisar etapa do RD: {stage_id} (Situação no Notion: '{notion_situacao}')")
        rd_leads_in_stage = fetch_rd_station_leads_by_stage(stage_id)
        if not rd_leads_in_stage: print("Nenhum lead encontrado nesta etapa."); continue
        for lead in rd_leads_in_stage:
            rd_lead_id, lead_name = lead["id"], lead.get("name", "Nome Desconhecido")
            lead_phone = ""
            if lead.get("contacts"):
                phones = (lead["contacts"][0].get("phones") or [{}])
                if phones: lead_phone = phones[0].get("phone")
            normalized_phone = re.sub(r'\D', '', str(lead_phone))
            page_info = rd_id_map.get(rd_lead_id)
            if not page_info and normalized_phone:
                page_info = phone_map.get(normalized_phone)
            if page_info:
                leads_a_atualizar.append(f"- *Atualizaria:* {lead_name} (ID: {rd_lead_id})")
            else:
                leads_a_criar.append(f"- *Criaria:* {lead_name} (Telefone: {normalized_phone})")

    print("\n--- A preparar o relatório da simulação ---")
    final_report = "🤖 *Relatório de Simulação da Sincronização RD -> Notion*\n\nNenhuma alteração foi feita na base de dados. Este é um resumo do que o script *teria* feito:\n\n---\n\n"
    if leads_a_criar:
        final_report += "✅ *Novos Leads a Serem Criados:*\n" + "\n".join(leads_a_criar)
    else:
        final_report += "✅ *Nenhum lead novo para criar.*\n"
    final_report += "\n\n---\n\n"
    if leads_a_atualizar:
        final_report += "🔄 *Leads Existentes a Serem Atualizados:*\n" + "\n".join(leads_a_atualizar)
    else:
        final_report += "🔄 *Nenhum lead existente para atualizar.*\n"
    if leads_a_criar or leads_a_atualizar:
        send_whatsapp_message(final_report)
    else:
        print("Nenhuma ação de criação ou atualização teria sido executada.")
    print("\n--- SCRIPT DE SIMULAÇÃO FINALIZADO ---")
