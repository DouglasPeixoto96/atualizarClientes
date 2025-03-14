from flask import Flask
import subprocess

app = Flask(__name__)

@app.route('/')
def home():
    return "Servidor rodando!"

@app.route('/update-db')
def update_db():
    try:
        # Executa o script e captura a saída
        process = subprocess.Popen(["python3", "atualizarPedidos.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()

        # Se houver erro, retorna erro
        if process.returncode != 0:
            return f"Erro na atualização:\n{stderr}", 500
        
        # Retorna a saída do script
        return f"Atualização concluída:\n{stdout}"

    except Exception as e:
        return f"Erro ao executar o script: {str(e)}", 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
