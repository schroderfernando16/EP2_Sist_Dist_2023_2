#PROBLEMAS A CORRIGIR, REPLICACAO (ok) E TRY_SERVER_LATER

import socket
import random
from datetime import datetime, timedelta # Importe a classe datetime aqui

class Message:
    def __init__(self, command, key, value=None, timestamp=None):
        self.command = command
        self.key = key
        self.value = value
        self.timestamp = timestamp

    def serialize(self):
        if self.value:
            return f"{self.command} {self.key} {self.value} {self.timestamp}"
        else:
            return f"{self.command} {self.key} {self.timestamp}"

    @staticmethod
    def deserialize(data):
        parts = data.split(" ")
        command = parts[0].upper()
        key = parts[1]
        value = " ".join(parts[2:-1]) if len(parts) > 3 else None
        timestamp = parts[-1]
        return Message(command, key, value, timestamp)

class Client:
    def __init__(self):
        self.servers = []
        self.timestamps = {}  # Dicionário para armazenar os timestamps do cliente

    def add_server(self, ip, port):
        self.servers.append((ip, port))

    def send_message(self, message):
        server_ip, server_port = random.choice(self.servers)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_ip, server_port))

        client_socket.send(message.serialize().encode())
        response = client_socket.recv(1024).decode()

        client_socket.close()

        return response

    def put(self, key, value):
        server_ip, server_port = random.choice(self.servers)
        timestamp = datetime.now()  # Obter o timestamp atual
        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # Converter o timestamp de volta para string
        message = Message("PUT", key, value, timestamp)
        response = self.send_message(message)

        if response.startswith("PUT_OK"):
            self.timestamps[key] = timestamp
            print(f"PUT_OK key: {key} value: {value} timestamp: {timestamp} realizada no servidor {server_ip}:{server_port}")

    def get(self, key):
        server_ip, server_port = random.choice(self.servers)
        if key in self.timestamps:
            timestamp_str = self.timestamps[key]  # Obtém o timestamp armazenado pelo cliente
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Converter o timestamp de volta para datetime
        else:
            timestamp = None

        random_seconds = random.randint(0,1)  # Gerar um valor aleatório de segundos entre 0 e 1
        timestamp += timedelta(seconds=random_seconds)  # Adicionar o valor aleatório de segundos ao timestamp
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # Converter o timestamp para string
        message = Message("GET", key, timestamp=timestamp_str)
        response = self.send_message(message)

        if response.startswith("TRY_OTHER_SERVER_OR_LATER"):
            print("TRY_OTHER_SERVER_OR_LATER")
        elif response == "NULL":
            print("Key not found.")
        else:
            parts = response.split(" ", 1)
            value = parts[0]
            timestamp_server = parts[1]
            self.timestamps[key] = timestamp_server
            print(f"GET key: {key} value: {value} obtido do servidor {server_ip}:{server_port}, meu timestamp: {self.timestamps[key]} e do servidor: {timestamp_server}")

    def run(self):
        while True:
            print("\nMenu:")
            print("1. PUT")
            print("2. GET")
            print("3. Exit")
            choice = input("Enter your choice: ")

            if choice == "1":
                key = input("Enter key: ")
                value = input("Enter value: ")
                self.put(key, value)

            elif choice == "2":
                key = input("Enter key: ")
                self.get(key)

            elif choice == "3":
                break

            else:
                print("Invalid choice. Try again.")

# Exemplo de uso do cliente
if __name__ == "__main__":
    client = Client()

    # Capturar IPs e portas dos servidores do teclado
    for _ in range(3):
        ip = input("Enter server IP: ")
        port = int(input("Enter server port: "))
        client.add_server(ip, port)

    client.run()






