import requests
import pymysql.cursors

# Configurações do banco de dados
DB_HOST = '177.153.63.44'
DB_USER = 'projecao_fut'
DB_PASSWORD = 'yELLOW@2020'
DB_DATABASE = 'projecao_fut'

# Função para obter o token de autenticação
def obter_token():
    url = "https://app.pontta.com/api/authenticate"
    data = {
        "email": "financeiro@i4mat.com.br",
        "password": "123456"
    }
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()["id_token"]
    except requests.exceptions.RequestException as err:
        print(f"Erro ao obter token de autenticação: {err}")
        return None

# Função para limpar uma tabela
def limpar_tabela(nome_tabela):
    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {nome_tabela}")
            connection.commit()
            print(f"Tabela '{nome_tabela}' limpa com sucesso.")
    except pymysql.MySQLError as e:
        print(f"Erro ao limpar a tabela '{nome_tabela}': {e}")
    finally:
        connection.close()

# Função para inserir dados em uma tabela
def inserir_dados(nome_tabela, colunas, dados):
    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
    try:
        with connection.cursor() as cursor:
            colunas_str = ', '.join(colunas)
            valores_str = ', '.join(['%s'] * len(colunas))
            sql = f"INSERT INTO {nome_tabela} ({colunas_str}) VALUES ({valores_str})"
            cursor.executemany(sql, dados)
            connection.commit()
            print(f"{cursor.rowcount} registros inseridos na tabela '{nome_tabela}' com sucesso.")
    except pymysql.MySQLError as e:
        print(f"Erro ao inserir dados na tabela '{nome_tabela}': {e}")
    finally:
        connection.close()

# Exportar dados da API 'api_receitas' para a tabela 'receita'
def exportar_dados_api_receitas(token):
    url = "https://api.pontta.com/api/revenues/summary"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        receitas = response.json()

        # Ajustar o formato do código removendo "Venda #"
        dados_receitas = [
            (item["description"].replace("Venda #", ""), item["value"]) 
            for item in receitas
        ]

        limpar_tabela("receita")
        inserir_dados("receita", ["Codigo", "Valor"], dados_receitas)
    except requests.exceptions.RequestException as e:
        print(f"Erro ao exportar dados de receitas: {e}")


# Exportar dados da API 'api_dados' para a tabela 'dados'
def exportar_dados_api_dados(token):
    url = "https://api.pontta.com/api/sales-orders/resume"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Extrair os dados incluindo deliveryDate
        dados = [
            (
                item["code"], 
                item["customer"]["name"], 
                item.get("endCustomerName", ""), 
                item.get("deliveryDate", "")  # Adiciona o campo deliveryDate
            )
            for item in response.json()
        ]
        
        # Limpar tabela e inserir dados
        limpar_tabela("dados")
        inserir_dados("dados", ["Codigo", "Cliente", "Cliente_Final", "Data_Entrega"], dados)
    
    except requests.exceptions.RequestException as e:
        print(f"Erro ao exportar dados de dados: {e}")

import requests

def exportar_dados_api_agrupamento(token):
    url = "https://api.pontta.com/api/production-groups/"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        dados_api = response.json()

        # Agora buscar todas as requisições aprovadas
        url_requisicoes = "https://api.pontta.com/api/purchase-requisitions/approveds"
        response_requisicoes = requests.get(url_requisicoes, headers=headers)
        response_requisicoes.raise_for_status()
        requisicoes = response_requisicoes.json()

        # Agrupar totais por ProductionGroupID
        totais_por_grupo = {}
        for item in requisicoes:
            group_id = item.get("reference", {}).get("PRODUCTIONGROUP_ID")
            code = item.get("code", "").strip().replace(" ", "").upper()
            quantity = item.get("quantity", {})
            valor = quantity.get("value", 0)

            if code.startswith("MDF") and group_id:
                if group_id not in totais_por_grupo:
                    totais_por_grupo[group_id] = {"branco": 0, "cores": 0}

                if code.endswith("34"):
                    totais_por_grupo[group_id]["branco"] += valor
                else:
                    totais_por_grupo[group_id]["cores"] += valor

        # Montar dados com as colunas adicionais "Branco" e "Cores"
        dados = []
        for item in dados_api:
            group_id = item["id"]
            branco = totais_por_grupo.get(group_id, {}).get("branco", 0)
            cores = totais_por_grupo.get(group_id, {}).get("cores", 0)

            dados.append((
                group_id,
                item["identifier"],
                item["finishDate"],
                item.get("resume", {}).get("controllableProductionItems", 0),
                branco,
                cores
            ))

        # Limpar tabela e inserir os dados com as novas colunas
        limpar_tabela("AGP")
        inserir_dados("AGP", ["ID", "AGP", "Data_Final", "Pecas", "Branco", "Cores"], dados)

    except requests.exceptions.RequestException as e:
        print(f"Erro ao exportar dados de dados: {e}")



# Atualizar a coluna 'Valor' na tabela 'dados' com base na tabela 'receita'
def atualizar_valores():
    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)
    try:
        with connection.cursor() as cursor:
            sql = """
                UPDATE dados d
                JOIN receita r ON d.Codigo = r.Codigo
                SET d.Valor = r.Valor
            """
            cursor.execute(sql)
            connection.commit()
            print(f"Coluna 'Valor' atualizada com sucesso na tabela 'dados'.")
    except pymysql.MySQLError as e:
        print(f"Erro ao atualizar a coluna 'Valor': {e}")
    finally:
        connection.close()


# Função principal
def atualizar_tabelas():
    token = obter_token()
    if not token:
        print("Não foi possível obter o token de autenticação.")
        return

    exportar_dados_api_receitas(token)
    exportar_dados_api_dados(token)
    exportar_dados_api_agrupamento(token)
    atualizar_valores()

# Executar o processo
if __name__ == "__main__":
    atualizar_tabelas()
