from flask import Flask
import subprocess

app = Flask(__name__)

@app.route('/')
def home():
    return "Servidor rodando!"

@app.route('/update-db')
def update_db():
    subprocess.run(["python3", "atualizarPedidos.py"])
    return "Atualização concluída!"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
