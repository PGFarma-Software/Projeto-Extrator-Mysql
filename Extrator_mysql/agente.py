# agent.py
from flask import Flask, request, jsonify
import threading
import subprocess
import sys
import os
import requests
import socket
import time
import logging
from logging_config import LoggingConfigurator  # Sua configuração de logging personalizada

# Configuração de logging para o agente
configurador = LoggingConfigurator(base_log_dir="logs/agente")
configurador.configurar_logging()

app = Flask(__name__)

# Identificação e endereço do servidor
MACHINE_ID = "MYSQL_TESTE"
SERVER_URL = "http://127.0.0.1:5000/api"

# Estado do sistema controlado pelo agente
status_sistema = {"status": "parado", "id": MACHINE_ID}
sistema_executando = False
sistema_process = None

def enviar_status():
    try:
        data = {"id": MACHINE_ID, "status": status_sistema["status"]}
        response = requests.post(f"{SERVER_URL}/status", json=data, timeout=5)
        if response.status_code == 200:
            logging.info(f"Status atualizado no servidor: {status_sistema['status']}")
        else:
            logging.warning(f"Erro ao atualizar status: {response.text}")
    except Exception as e:
        logging.error(f"Erro ao conectar no servidor para atualizar status: {e}")

def registrar_status():
    global sistema_executando
    try:
        response = requests.get(f"{SERVER_URL}/machines", timeout=5)
        if response.status_code == 200:
            maquinas = response.json()
            if MACHINE_ID in maquinas and maquinas[MACHINE_ID]["status"] == "rodando":
                sistema_executando = True
                status_sistema["status"] = "rodando"
            else:
                sistema_executando = False
                status_sistema["status"] = "parado"
    except Exception as e:
        logging.error(f"Erro ao registrar status: {e}")
    enviar_status()

@app.route("/status", methods=["GET"])
def obter_status():
    return jsonify(status_sistema)

@app.route("/start", methods=["POST"])
def iniciar_sistema():
    global sistema_executando, sistema_process
    if sistema_executando:
        return jsonify({"message": "Sistema já rodando."}), 400

    logging.info("Comando de INÍCIO recebido.")
    sistema_executando = True
    status_sistema["status"] = "rodando"
    enviar_status()
    try:
        sistema_process = subprocess.Popen([sys.executable, "main.py"])
        logging.info(f"Sistema iniciado com PID {sistema_process.pid}.")
        threading.Thread(target=monitor_sistema, daemon=True).start()
    except Exception as e:
        logging.error(f"Erro ao iniciar sistema: {e}")
        sistema_executando = False
        status_sistema["status"] = "parado"
        enviar_status()
        return jsonify({"message": "Falha ao iniciar."}), 500
    return jsonify({"message": f"Sistema iniciado na máquina {MACHINE_ID}."})

@app.route("/stop", methods=["POST"])
def parar_sistema():
    global sistema_executando, sistema_process
    if not sistema_executando:
        return jsonify({"message": "Sistema já parado."}), 400

    logging.info("Comando de PARADA recebido.")
    if sistema_process:
        try:
            sistema_process.terminate()
            sistema_process.wait(timeout=10)
            logging.info("Sistema principal parado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao parar o sistema: {e}")
            return jsonify({"message": "Erro ao parar o sistema."}), 500
        finally:
            sistema_process = None
    sistema_executando = False
    status_sistema["status"] = "parado"
    enviar_status()
    return jsonify({"message": f"Sistema parado na máquina {MACHINE_ID}."})

def monitor_sistema():
    global sistema_executando, sistema_process
    if sistema_process:
        retcode = sistema_process.wait()
        logging.info(f"Sistema finalizado com código {retcode}.")
        sistema_executando = False
        status_sistema["status"] = "parado"
        enviar_status()
        sistema_process = None

def verificar_comandos():
    while True:
        try:
            response = requests.get(f"{SERVER_URL}/machines", timeout=5)
            if response.status_code == 200:
                maquinas = response.json()
                if MACHINE_ID in maquinas:
                    comando = maquinas[MACHINE_ID].get("command")
                    logging.debug(f"Dados da máquina {MACHINE_ID}: {maquinas[MACHINE_ID]}")
                    if comando:
                        logging.info(f"Comando recebido: {comando}")
                        if comando == "start":
                            try:
                                r = requests.post("http://127.0.0.1:5001/start", timeout=5)
                                logging.info(f"Resposta /start: {r.text}")
                            except Exception as ex:
                                logging.error(f"Erro em /start: {ex}")
                        elif comando == "stop":
                            try:
                                r = requests.post("http://127.0.0.1:5001/stop", timeout=5)
                                logging.info(f"Resposta /stop: {r.text}")
                            except Exception as ex:
                                logging.error(f"Erro em /stop: {ex}")
                        try:
                            requests.post(f"{SERVER_URL}/command", json={"id": MACHINE_ID, "command": None}, timeout=5)
                        except Exception as ex:
                            logging.error(f"Erro ao limpar comando: {ex}")
        except Exception as e:
            logging.error(f"Erro ao buscar comandos: {e}")
        time.sleep(2)

def iniciar_agente():
    logging.info(f"Agente iniciado em http://127.0.0.1:5001")
    threading.Thread(target=verificar_comandos, daemon=True).start()
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True, use_reloader=False)

if __name__ == "__main__":
    registrar_status()
    iniciar_agente()
