import requests
import pymysql.cursors
from concurrent.futures import ThreadPoolExecutor

# Configurações do banco de dados
DB_HOST = '177.153.63.44'
DB_USER = 'projecao_fut'
DB_PASSWORD = 'yELLOW@2020'
DB_DATABASE = 'projecao_fut'

# Obter token
def obter_token():
    try:
        response = requests.post("https://app.pontta.com/api/authenticate", json={
            "email": "financeiro@i4mat.com.br",
            "password": "123456"
        })
        response.raise_for_status()
        return response.json()["id_token"]
    except:
        return None

# Limpar tabela
def limpar_tabela(nome_tabela, conn):
    with conn.cursor() as cursor:
        cursor.execute(f"DELETE FROM `{nome_tabela}`")
    conn.commit()

# Inserir dados
def inserir_dados(nome_tabela, colunas, dados, conn):
    if not dados: return
    with conn.cursor() as cursor:
        sql = f"INSERT INTO `{nome_tabela}` ({', '.join(colunas)}) VALUES ({', '.join(['%s'] * len(colunas))})"
        cursor.executemany(sql, dados)
    conn.commit()

# Atualizar valor das receitas
def atualizar_valores(conn):
    with conn.cursor() as cursor:
        sql = """
            UPDATE dados d
            JOIN receita r ON d.Codigo = r.Codigo
            SET d.Valor = r.Valor
        """
        cursor.execute(sql)
    conn.commit()

# Receita
def exportar_receitas(token, conn):
    try:
        r = requests.get("https://api.pontta.com/api/revenues/summary", headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        receitas = [(item["description"].replace("Venda #", ""), item["value"]) for item in r.json()]
        limpar_tabela("receita", conn)
        inserir_dados("receita", ["Codigo", "Valor"], receitas, conn)
        return True
    except:
        return False

# Dados
def exportar_dados(token, conn):
    try:
        r = requests.get("https://api.pontta.com/api/sales-orders/resume", headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        dados = [
            (
                item["code"],
                item["customer"]["name"],
                item.get("endCustomerName", ""),
                item.get("deliveryDate", "")
            )
            for item in r.json()
        ]
        limpar_tabela("dados", conn)
        inserir_dados("dados", ["Codigo", "Cliente", "Cliente_Final", "Data_Entrega"], dados, conn)
        return True
    except:
        return False

# Agrupamento
def exportar_agrupamento(token, conn):
    try:
        headers = {"Authorization": f"Bearer {token}"}
        dados_api = requests.get("https://api.pontta.com/api/production-groups/", headers=headers).json()
        requisicoes = requests.get("https://api.pontta.com/api/purchase-requisitions/approveds", headers=headers).json()

        totais_por_grupo = {}
        for item in requisicoes:
            group_id = item.get("reference", {}).get("PRODUCTIONGROUP_ID")
            code = item.get("code", "").strip().replace(" ", "").upper()
            valor = item.get("quantity", {}).get("value", 0)
            if code.startswith("MDF") and group_id:
                if group_id not in totais_por_grupo:
                    totais_por_grupo[group_id] = {"branco": 0, "cores": 0}
                if code.endswith("34"):
                    totais_por_grupo[group_id]["branco"] += valor
                else:
                    totais_por_grupo[group_id]["cores"] += valor

        dados = []
        for item in dados_api:
            group_id = item["id"]
            dados.append((
                group_id,
                item["identifier"],
                item["finishDate"],
                item.get("resume", {}).get("controllableProductionItems", 0),
                totais_por_grupo.get(group_id, {}).get("branco", 0),
                totais_por_grupo.get(group_id, {}).get("cores", 0)
            ))

        limpar_tabela("AGP", conn)
        inserir_dados("AGP", ["ID", "AGP", "Data_Final", "Pecas", "Branco", "Cores"], dados, conn)
        return True
    except:
        return False

# Função principal
def atualizar_tabelas():
    token = obter_token()
    if not token:
        print("Erro: Não foi possível autenticar.")
        return

    try:
        conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)

        with ThreadPoolExecutor() as executor:
            f1 = executor.submit(exportar_receitas, token, conn)
            f2 = executor.submit(exportar_dados, token, conn)
            f3 = executor.submit(exportar_agrupamento, token, conn)

            receitas_ok = f1.result()
            dados_ok = f2.result()
            agp_ok = f3.result()

        atualizar_valores(conn)
        conn.close()

        if receitas_ok and dados_ok and agp_ok:
            print("Atualização concluída com sucesso.")
        else:
            print("Alguma etapa falhou.")

    except Exception as e:
        print(f"Erro geral: {e}")

# Executar
if __name__ == "__main__":
    atualizar_tabelas()
