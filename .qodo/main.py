import psycopg2
import json
from datetime import datetime, date
from firebase_admin import credentials, initialize_app, messaging

# -----------------------------
# Configurações PostgreSQL
# -----------------------------
DB_CONFIG = {
    "host": "aws-0-sa-east-1.pooler.supabase.com",
    "port": 5432,
    "database": "postgres",
    "user": "postgres.kjitfbxtgoezcztbituj",
    "password": "ctJY7pzRQ3HZjpgU"
}

# -----------------------------
# Inicializa Firebase Admin
# -----------------------------
cred = credentials.Certificate("serviceAccount.json")  # JSON da conta de serviço
initialize_app(cred)

# -----------------------------
# Conecta no PostgreSQL
# -----------------------------
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# -----------------------------
# Busca tokens ativos
# -----------------------------
cur.execute("SELECT usuario_id, token FROM device_tokens WHERE active = true")
tokens_db = cur.fetchall()
tokens_json = [{"usuario_id": t[0], "token": t[1]} for t in tokens_db]

print("TOKENS ATIVOS:")
print(json.dumps(tokens_json, indent=2))

# -----------------------------
# Busca cobranças pendentes
# -----------------------------
cur.execute("""
    SELECT documentoparcela, vencimento, pessoaid, descricaocontarecebersituacao
    FROM cobrancas_microwork
    WHERE descricaocontarecebersituacao != 'QUITADO'
""")
cobrancas_db = cur.fetchall()
cobrancas = [
    {
        "documentoparcela": c[0],
        "vencimento": c[1].isoformat() if isinstance(c[1], date) else c[1],
        "pessoaid": c[2],
        "descricaocontarecebersituacao": c[3]
    }
    for c in cobrancas_db
]


# -----------------------------
# Gera JSON de notificações
# -----------------------------
diasAlvo = [5, 0, -1, -2, -3, -5, -6]
hoje = date.today()
notificacoes = []

# Cria um mapeamento external_reference -> id
cur.execute("SELECT id, external_reference FROM usuarios")
usuarios_db = cur.fetchall()
external_to_id = {u[1]: u[0] for u in usuarios_db}  # {external_reference: id}

# Loop de notificações
notificacoes = []

for c in cobrancas:
    venc = datetime.fromisoformat(c["vencimento"]).date()
    dias = (venc - hoje).days
    if dias not in diasAlvo:
        continue

    msg = ""
    if dias > 0:
        msg = f"Faltam {dias} dias para sua cobrança vencer."
    elif dias == 0:
        msg = "Hoje é o vencimento de uma cobrança."
    else:
        msg = f"Você está com uma cobrança atrasada há {abs(dias)} dias."

    # Pega o id do usuário pelo external_reference (pessoaid)
    usuario_id = external_to_id.get(c["pessoaid"])
    if not usuario_id:
        continue

    # Todos os tokens do usuário
    user_tokens = [t["token"] for t in tokens_json if t["usuario_id"] == usuario_id]

    for token in user_tokens:
        notif = {
            "token": token,
            "title": "Cobrança Pendente",
            "body": msg,
            "documentoparcela": c["documentoparcela"]
        }
        notificacoes.append(notif)


print("\nJSON FINAL DE NOTIFICAÇÕES:")
print(json.dumps(notificacoes, indent=2))

# -----------------------------
# Envia notificações via FCM
# -----------------------------
for n in notificacoes:
    message = messaging.Message(
        token=n["token"],
        notification=messaging.Notification(
            title=n["title"],
            body=n["body"]
        )
    )
    try:
        print("Tentando...")
        response = messaging.send(message)
        print(f"Notificação enviada: {response}")
    except Exception as e:
        print(f"Erro enviando notificação: {e}")

# -----------------------------
# Fecha conexão
# -----------------------------
cur.close()
conn.close()
