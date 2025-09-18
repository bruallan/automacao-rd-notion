# -*- coding: utf-8 -*-
"""
RD Station CRM -> Notion (e alertas via BotConversa)
Versão corrigida: 2025-09-17

Melhorias:
- Comparação correta de valores Notion (payload de envio x objeto de resposta)
- Fallback para custom fields do RD (deal_custom_fields/custom_fields/fields)
- Normalização de telefone em E.164 (+55DDDNÚMERO)
- Backup do Notion com page_id e last_edited_time
- Timeouts e retries para requests
"""

# --- BIBLIOTECAS ---
import os
import requests
import re
import csv
import datetime
import json
import time
from requests.adapters import HTTPAdapter, Retry
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CONFIGURAÇÕES GERAIS ---
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()
RD_CRM_TOKEN = os.environ.get("RD_CRM_TOKEN", "").strip()

# --- CONFIGURAÇÕES DO WHATSAPP ---
BOTCONVERSA_API_KEY = os.environ.get("BOTCONVERSA_API_KEY", "").strip()
BOTCONVERSA_SUBSCRIBER_ID = os.environ.get("BOTCONVERSA_SUBSCRIBER_ID", "").strip()
BOTCONVERSA_BASE_URL = "https://backend.botconversa.com.br"

# --- CONFIGURAÇÕES DO GOOGLE DRIVE ---
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()
GDRIVE_CREDENTIALS_JSON = os.environ.get("GDRIVE_CREDENTIALS_JSON", "").strip()
GDRIVE_TOKEN_JSON = os.environ.get("GDRIVE_TOKEN_JSON", "").strip()

# --- HTTP SESSION (timeouts + retries) ---
def make_session():
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(['GET', 'POST', 'PATCH'])
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s
SESSION = make_session()
REQUEST_TIMEOUT = 15  # segundos

# --- MAPEAMENTOS ---
RD_STAGES_MAP = {
    "67ae261cab5a8e00178ea863": "Avaliando",
    "67bcd1b67d60d4001b8c8aa2": "Condicionado",
    "67ae261cab5a8e00178ea864": "Aprovado",
    "67ae261cab5a8e00178ea865": "Com Reserva",
}

NOTION_RD_MAP = {
    "67ea8afafddd15001447f639": {"notion_name": "ID (RD Station)", "notion_type": "text"},
    "67b62f4fad0a4e0014841510": {"notion_name": "De onde é?", "notion_type": "text"},
    "67b31f6fcbd9da00143b9e4e": {"notion_name": "Aluguel", "notion_type": "number"},
    "67bdbe2a5062a6001945f18b": {"notion_name": "Por que deseja a casa?", "notion_type": "text"},
    "67b31ac9fce8b4001e8dca11": {"notion_name": "Recebe Bolsa Família?", "notion_type": "select"},
    "67b0a3f6b436410018d97957": {"notion_name": "Profissão", "notion_type": "text"},
    "67b31e93786c3f00143b07ce": {"notion_name": "Idade", "notion_type": "number"},
    "67b321ba30fafb001c8f8743": {"notion_name": "Estado Civil", "notion_type": "text"},
    "67b5d8552b873a001c9cca66": {"notion_name": "Dependente", "notion_type": "select"},
    "67b31f37ca237d001e358c1b": {"notion_name": "+3 anos CLT", "notion_type": "select"},
    "680cadbefcff56001b6be1a8": {"notion_name": "CPF (COMPRADOR)", "notion_type": "text"},
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

# --- FUNÇÕES: GOOGLE DRIVE (backup) ---
def upload_to_google_drive(filename):
    print(f"--- A iniciar upload do backup para o Google Drive: '{filename}' ---")
    try:
        if not GDRIVE_CREDENTIALS_JSON or not GDRIVE_TOKEN_JSON:
            print("### ERRO: Credenciais/token do Google Drive não encontrados. Verifique os Secrets. ###")
            return
        token_info = json.loads(GDRIVE_TOKEN_JSON)
        creds = google.oauth2.credentials.Credentials.from_authorized_user_info(
            token_info, scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': filename, 'parents': [GDRIVE_FOLDER_ID]} if GDRIVE_FOLDER_ID else {'name': filename}
        media = MediaFileUpload(filename, mimetype='text/csv')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✔ Backup carregado para o Google Drive! ID: {file.get('id')}")
    except Exception as e:
        print(f"### ERRO AO FAZER UPLOAD DO BACKUP PARA O GOOGLE DRIVE: {e} ###")

def extract_backup_property_value(prop):
    prop_type = prop.get('type')
    if not prop_type:
        return ""
    try:
        if prop_type in ['title', 'rich_text']:
            return prop[prop_type][0]['text']['content'] if prop.get(prop_type) else ""
        elif prop_type == 'number':
            return prop['number']
        elif prop_type == 'select':
            return prop['select']['name'] if prop.get('select') else ""
        elif prop_type == 'multi_select':
            return ", ".join([item['name'] for item in prop.get('multi_select', [])])
        elif prop_type == 'date':
            return prop['date']['start'] if prop.get('date') else ""
        elif prop_type == 'phone_number':
            return prop.get('phone_number') or ""
    except Exception:
        return ""
    return ""

def backup_notion_database():
    print("--- A iniciar o backup da base de dados do Notion ---")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    all_pages = []
    has_more, next_cursor = True, None
    while has_more:
        payload = {}
        if next_cursor:
            payload['start_cursor'] = next_cursor
        try:
            response = SESSION.post(url, headers=NOTION_HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            all_pages.extend(data['results'])
            has_more, next_cursor = data.get('has_more'), data.get('next_cursor')
        except requests.RequestException as e:
            print(f"### ERRO AO BUSCAR DADOS DO NOTION PARA BACKUP: {e} ###")
            return

    if not all_pages:
        print("A base de dados do Notion está vazia. Backup não gerado.")
        return

    processed_data, all_headers = [], set()
    temp_processed = []
    for page in all_pages:
        row = {}
        # adições para auditoria:
        row["__page_id"] = page.get("id", "")
        row["__last_edited_time"] = page.get("last_edited_time", "")
        all_headers.update(["__page_id", "__last_edited_time"])
        for prop_name, prop_data in page.get('properties', {}).items():
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
        try:
            if os.path.exists(filename):
                os.remove(filename)
                print(f"Ficheiro temporário '{filename}' apagado.")
        except Exception:
            pass

# --- FUNÇÕES DE WHATSAPP ---
def send_whatsapp_message(message):
    """
    Envia a mensagem para uma lista de IDs de subscritores do BotConversa.
    """
    if not BOTCONVERSA_API_KEY or not BOTCONVERSA_SUBSCRIBER_ID:
        print("!! Aviso: API Key ou ID do Subscritor do BotConversa não configurados. Mensagem não enviada.")
        return

    subscriber_ids = [id.strip() for id in BOTCONVERSA_SUBSCRIBER_ID.split(',') if id.strip()]
    print(f"   -> A iniciar o envio de mensagens para {len(subscriber_ids)} contato(s).")

    for subscriber_id in subscriber_ids:
        url = f"{BOTCONVERSA_BASE_URL}/api/v1/webhook/subscriber/{subscriber_id}/send_message/"
        headers = {"Content-Type": "application/json", "API-KEY": BOTCONVERSA_API_KEY}
        payload = {"type": "text", "value": message}
        try:
            print(f"   -> Enviando mensagem para o subscritor ID: {subscriber_id}")
            response = SESSION.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            print(f"   - Mensagem para {subscriber_id} enviada com sucesso.")
        except requests.RequestException as e:
            print(f"   ### ERRO ao enviar mensagem para o ID {subscriber_id}: {e}")
        time.sleep(1)

# --- AUXILIARES NOTION ---
def format_notion_property(value, notion_type):
    if value is None or str(value).strip() == "":
        return None
    try:
        if notion_type == "text":
            return {"rich_text": [{"text": {"content": str(value)}}]}

        elif notion_type == "number":
            s = str(value).strip()

            # tratar valores "vazios"
            if s in ("", "-", "–", "—", "N/A", "n/a", "NA", "null", "None"):
                return None

            # remove moeda/espaços
            s = s.replace("R$", "").replace(" ", "")

            # normaliza separadores:
            # se tem ponto e vírgula, assume ponto como milhar e vírgula como decimal
            if "." in s and "," in s:
                s = s.replace(".", "").replace(",", ".")
            else:
                # se só vírgula, trata como decimal
                s = s.replace(",", ".")

            # mantém apenas dígitos, ponto e sinal
            cleaned = re.sub(r"[^\d.\-]", "", s)

            # valida formato numérico (inteiro/decimal, com opcional sinal)
            if re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
                return {"number": float(cleaned)}
            else:
                return None

        elif notion_type == "select":
            return {"select": {"name": str(value)}}

    except (ValueError, TypeError) as e:
        print(f"  !! Aviso: Não foi possível formatar '{value}' para '{notion_type}'. Erro: {e}")
        return None

    return None


def _get_simple_value_from_prop(prop_object):
    """Extrai valor simples de um objeto de PROPRIEDADE do Notion (resposta da API, contém 'type')."""
    if not prop_object:
        return None
    prop_type = prop_object.get('type')
    try:
        if prop_type in ['title', 'rich_text']:
            prop_value = prop_object.get(prop_type)
            return prop_value[0]['text']['content'] if prop_value else None
        elif prop_type == 'number':
            return prop_object.get('number')
        elif prop_type == 'select':
            select_obj = prop_object.get('select')
            return select_obj.get('name') if select_obj else None
        elif prop_type == 'multi_select':
            items = prop_object.get('multi_select', [])
            return ", ".join([it['name'] for it in items]) if items else None
        elif prop_type == 'phone_number':
            return prop_object.get('phone_number')
    except Exception:
        return None
    return None

def _extract_value_from_notion_payload(prop_obj):
    """
    Extrai valor simples de um OBJETO DE ENVIO ao Notion (não tem 'type').
    Usado para comparação no update antes de PATCH.
    """
    if not isinstance(prop_obj, dict):
        return ""
    try:
        if "title" in prop_obj:
            arr = prop_obj["title"]
            return arr[0]["text"]["content"] if arr else ""
        if "rich_text" in prop_obj:
            arr = prop_obj["rich_text"]
            return arr[0]["text"]["content"] if arr else ""
        if "select" in prop_obj:
            sel = prop_obj["select"]
            return (sel or {}).get("name", "")
        if "multi_select" in prop_obj:
            return ", ".join([it["name"] for it in (prop_obj["multi_select"] or [])])
        if "number" in prop_obj:
            return str(prop_obj["number"])
        if "phone_number" in prop_obj:
            return prop_obj.get("phone_number") or ""
    except Exception:
        return ""
    return ""

# --- TELEFONE (E.164) ---
def normalize_phone_e164_br(phone_str):
    """
    Normaliza para +55DDDNÚMERO (preserva nono dígito).
    Aceita entradas com 55/00/+, espaços, etc.
    """
    if not phone_str:
        return ""
    digits = re.sub(r"\D", "", str(phone_str))
    # remove prefixos comuns
    if digits.startswith("00"):
        digits = digits[2:]
    # já começa com 55?
    if digits.startswith("55"):
        core = digits[2:]
    else:
        core = digits
    # tenta garantir 10/11 dígitos
    if len(core) in (10, 11):
        return "+55" + core
    # fallback: se já veio completo com 55 + 10/11
    if len(digits) in (12, 13) and digits.startswith("55"):
        return "+" + digits
    # último caso: prefixa +55
    return "+55" + core

# --- RD: custom fields fallback ---
def _get_deal_custom_fields_dict(deal):
    """
    Gera dict {custom_field_id: value} tentando várias chaves possíveis.
    """
    if not isinstance(deal, dict):
        return {}
    # candidatos de chaves
    for key in ("deal_custom_fields", "custom_fields", "fields"):
        lst = deal.get(key)
        if isinstance(lst, list) and lst:
            out = {}
            for it in lst:
                try:
                    cf = it.get("custom_field") or {}
                    cf_id = cf.get("_id") or cf.get("id") or cf.get("uuid")
                    val = it.get("value")
                    if cf_id:
                        out[cf_id] = val
                except Exception:
                    continue
            if out:
                return out
    return {}

# --- BUILD de propriedades para o Notion ---
def build_properties_payload(lead_data, situacao):
    """Constrói o dicionário de propriedades para a API do Notion, apenas com valores presentes."""
    properties = {}
    properties["Nome (Completar)"] = {"title": [{"text": {"content": lead_data.get("name", "Negociação sem nome")}}]}
    properties["ID (RD Station)"] = {"rich_text": [{"text": {"content": lead_data.get("id", "")}}]}
    properties["Status"] = {"multi_select": [{"name": situacao}]}

    # telefone
    lead_phone = ""
    try:
        if lead_data.get("contacts"):
            phones = (lead_data["contacts"][0].get("phones") or [{}])
            if phones:
                lead_phone = phones[0].get("phone")
    except Exception:
        pass
    properties["Telefone"] = {"phone_number": normalize_phone_e164_br(lead_phone) if lead_phone else None}

    # custom fields
    custom_fields_dict = _get_deal_custom_fields_dict(lead_data)

    for rd_id, notion_info in NOTION_RD_MAP.items():
        rd_value = custom_fields_dict.get(rd_id)
        # NÃO sobrescreve com vazio
        if rd_value is not None and str(rd_value).strip() != "":
            if notion_info["notion_name"] == "ID (RD Station)":
                continue
            formatted_property = format_notion_property(rd_value, notion_info["notion_type"])
            if formatted_property:
                properties[notion_info["notion_name"]] = formatted_property

    return properties

# --- Busca leads existentes no Notion (maps por RD_ID e Telefone) ---
def get_existing_notion_leads():
    """Busca páginas e mapeia por RD ID e por Telefone (E.164)."""
    print("A buscar leads existentes no Notion para mapeamento...")
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    rd_id_map, phone_map = {}, {}
    has_more, next_cursor = True, None

    while has_more:
        payload = {}
        if next_cursor:
            payload['start_cursor'] = next_cursor
        try:
            response = SESSION.post(url, headers=NOTION_HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                print(f"### ERRO ao buscar leads do Notion: {response.text}")
                return {}, {}
            data = response.json()
        except requests.RequestException as e:
            print(f"### ERRO de rede Notion: {e}")
            return {}, {}

        for page in data.get("results", []):
            props = page.get("properties", {})
            # Mapeia por ID do RD
            rd_id_prop = props.get("ID (RD Station)")
            rd_id = _get_simple_value_from_prop(rd_id_prop)
            if rd_id:
                rd_id_map[rd_id] = page  # guarda página completa

            # Mapeia por Telefone
            phone_prop = props.get("Telefone")
            raw_phone = _get_simple_value_from_prop(phone_prop)
            norm_phone = normalize_phone_e164_br(raw_phone)
            if norm_phone:
                phone_map[norm_phone] = page  # guarda página completa

        has_more, next_cursor = data.get('has_more'), data.get('next_cursor')

    print(f"Encontrados {len(rd_id_map)} leads com ID do RD e {len(phone_map)} leads com telefone no Notion.")
    return rd_id_map, phone_map

# --- RD CRM: busca deals por etapa (v1) ---
def fetch_rd_station_leads_by_stage(stage_id):
    url = f"https://crm.rdstation.com/api/v1/deals?token={RD_CRM_TOKEN}&deal_stage_id={stage_id}"
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json().get("deals", [])
    except requests.RequestException as e:
        print(f"Erro ao buscar negociações da etapa {stage_id} no RD Station: {e}")
        return []

# --- UPDATE/CREATE Notion ---
def update_lead_in_notion(notion_page_data, lead_data, situacao):
    """Compara campos e envia somente diffs. Evita sobrescrever com vazio."""
    lead_name = lead_data.get("name", "Nome Desconhecido")
    notion_page_id = notion_page_data["id"]
    print(f"  -> A ATUALIZAR lead no Notion: '{lead_name}'")

    url = f"https://api.notion.com/v1/pages/{notion_page_id}"

    new_properties_payload = build_properties_payload(lead_data, situacao)
    old_properties = notion_page_data.get("properties", {})

    # diffs (comparar com extrator de PAYLOAD)
    changes_list = []
    for prop_name, new_prop_obj in new_properties_payload.items():
        if prop_name == "Status":
            continue  # tratamos status separado
        old_prop_obj = old_properties.get(prop_name)
        old_value = _get_simple_value_from_prop(old_prop_obj)
        new_value = _extract_value_from_notion_payload(new_prop_obj)
        if str(old_value) != str(new_value):
            changes_list.append(f"- *{prop_name}:* de '{old_value or 'vazio'}' para '{new_value}'")

    # status (não sobrescreve se houver divergência local intencional)
    current_status = _get_simple_value_from_prop(old_properties.get("Status"))
    status_divergence = current_status and current_status != situacao
    if status_divergence:
        print(f"  !! Aviso: Status no Notion ('{current_status}') ≠ esperado ('{situacao}'). Não será alterado.")
        if "Status" in new_properties_payload:
            del new_properties_payload["Status"]
        alert_message = (
            f"⚠️ *Alerta de Sincronização*\n\n"
            f"O lead *{lead_name}* teve uma divergência de status.\n\n"
            f"*- Status no Notion:* {current_status}\n"
            f"*- Etapa no RD (esperado):* {situacao}\n"
        )
        if changes_list:
            alert_message += "\n*Outras Alterações Realizadas:*\n" + "\n".join(changes_list)
        send_whatsapp_message(alert_message)

    # Decide se atualiza
    payload_sem_status = {k: v for k, v in new_properties_payload.items() if k != "Status"}
    deve_atualizar_status = not status_divergence and current_status != situacao
    deve_atualizar_outros = bool(changes_list)

    if deve_atualizar_status or deve_atualizar_outros:
        payload_final = new_properties_payload if deve_atualizar_status else payload_sem_status
        try:
            response = SESSION.patch(url, headers=NOTION_HEADERS, json={"properties": payload_final}, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return f"- Lead atualizado: *{lead_name}*\n  ({len(changes_list)} campos alterados)"
            else:
                print(f"  ### ERRO ao atualizar lead no Notion: {response.status_code} {response.text}")
                return None
        except requests.RequestException as e:
            print(f"  ### ERRO de rede ao atualizar Notion: {e}")
            return None
    else:
        print("  -> Nenhuma alteração detectada para este lead.")
        return None

def create_lead_in_notion(lead_data, situacao):
    lead_name = lead_data.get("name", "Negociação sem nome")
    print(f"  -> A CRIAR novo lead no Notion: '{lead_name}'")
    url = "https://api.notion.com/v1/pages"
    properties_payload = build_properties_payload(lead_data, situacao)
    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties_payload}
    try:
        response = SESSION.post(url, headers=NOTION_HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            lead_phone = properties_payload.get("Telefone", {}).get("phone_number", "N/A")
            return (f"*Lead Adicionado*\n- *Nome:* {lead_name}\n- *Telefone:* {lead_phone}\n- *Status:* {situacao}")
        else:
            print(f"  ### ERRO ao criar lead no Notion: {response.status_code} {response.text}")
            return None
    except requests.RequestException as e:
        print(f"  ### ERRO de rede ao criar lead no Notion: {e}")
        return None

# --- FLUXO PRINCIPAL (MODO DE PRODUÇÃO / BATCH) ---
if __name__ == "__main__":
    # Opcional: descomente para rodar backup antes de qualquer alteração
    # backup_notion_database()

    print("\n--- A INICIAR SINCRONIZAÇÃO RD -> NOTION (MODO DE PRODUÇÃO) ---")
    created_leads_summary, updated_leads_summary = [], []
    rd_id_map, phone_map = get_existing_notion_leads()

    for stage_id, notion_situacao in RD_STAGES_MAP.items():
        print(f"\nA processar etapa do RD: {stage_id} (Situação no Notion: '{notion_situacao}')")
        rd_leads_in_stage = fetch_rd_station_leads_by_stage(stage_id)
        if not rd_leads_in_stage:
            print("Nenhum lead encontrado nesta etapa.")
            continue

        print(f"Encontrados {len(rd_leads_in_stage)} leads.")
        for lead in rd_leads_in_stage:
            rd_lead_id = lead.get("id")
            lead_name = lead.get("name", "Nome Desconhecido")

            # telefone
            lead_phone = ""
            try:
                if lead.get("contacts"):
                    phones = (lead["contacts"][0].get("phones") or [{}])
                    if phones:
                        lead_phone = phones[0].get("phone")
            except Exception:
                pass
            normalized_phone = normalize_phone_e164_br(lead_phone)

            print(f"\n--- A processar Lead: {lead_name} (ID RD: {rd_lead_id}) ---")

            page_data = rd_id_map.get(rd_lead_id)
            if not page_data and normalized_phone:
                page_data = phone_map.get(normalized_phone)

            if page_data:
                summary = update_lead_in_notion(page_data, lead, notion_situacao)
                if summary:
                    updated_leads_summary.append(summary)
            else:
                summary = create_lead_in_notion(lead, notion_situacao)
                if summary:
                    created_leads_summary.append(summary)

    print("\n--- A preparar o relatório final da sincronização ---")
    final_report = "🤖 *Relatório da Sincronização RD -> Notion*\n\n"
    if created_leads_summary:
        final_report += "✅ *Novos Leads Adicionados ao Notion*\n\n" + "\n\n".join(created_leads_summary)
    if updated_leads_summary:
        if final_report != "🤖 *Relatório da Sincronização RD -> Notion*\n\n":
            final_report += "\n\n---\n\n"
        final_report += "🔄 *Leads Existentes que Foram Atualizados*\n\n" + "\n".join(updated_leads_summary)

    if created_leads_summary or updated_leads_summary:
        send_whatsapp_message(final_report)
    else:
        final_report += "✅ Nenhuma alteração foi realizada nesta execução."
        send_whatsapp_message(final_report)
        print("Nenhuma alteração foi realizada. Relatório de 'sem alterações' enviado.")

    print("\n--- SCRIPT DE SINCRONIZAÇÃO FINALIZADO ---")
