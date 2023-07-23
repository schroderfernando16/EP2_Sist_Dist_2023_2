#Versão final do código do Cliente
#EP2 Sistemas Distribuidos UFABC 2023.2
#Fernando Schroder Rodrigues
#RA:11201921885


import socket
import random
from datetime import datetime, timedelta 


'''Criação da classe Message,  foi utilizado o método de serialização para envio e recebimento das mensagens entre servidores e clientes'''
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

'''Classe do cliente, aqui o cliente ira fazer as requisição aos servidores, ele pega os inputs pelo teclado de 3 IPs e 3 portas dos servidores '''
class Client:
    def __init__(self):
        self.servers = []
        self.timestamps = {} 
    '''Função de INIT, onde as portas e os IPs capturados pelo teclado serão armazenados pelo cliente'''
    def add_server(self, ip, port):
        self.servers.append((ip, port))
    '''Função send message, Usando o protocolo TCP para que seja possível a comunicação por sockets entre os componentes do sistema '''
    def send_message(self, message):
        server_ip, server_port = random.choice(self.servers)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((server_ip, server_port))

        client_socket.send(message.serialize().encode())
        response = client_socket.recv(1024).decode()

        client_socket.close()

        return response
    '''Função PUT: Aqui o cliente irá mandar a requisição de PUT de uma chave e um valor capturados pelo teclado, além de pegar o timestamp do cliente através da biblioteca datetime '''
    def put(self, key, value):
        server_ip, server_port = random.choice(self.servers)
        timestamp = datetime.now()  # Obter o timestamp atual
        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # Converter o timestamp de volta para string
        message = Message("PUT", key, value, timestamp)
        response = self.send_message(message)

        if response.startswith("PUT_OK"):
            self.timestamps[key] = timestamp
            print(f"PUT_OK key: {key} value: {value} timestamp: {timestamp} realizada no servidor {server_ip}:{server_port}")
    '''Função GET: Cliente envia a um servidor random uma chave e o timestamp armazenado, por conta de um dos requisitos do projeto foi criada uma função para adicionar segundos
     timestamp aleatoriamente para conseguir entrar na condicional do TRY_OTHER_SERVER_OR_LATER e testar as falhas de requisição do servidor'''
    def get(self, key):
        server_ip, server_port = random.choice(self.servers)
        if key in self.timestamps:
            timestamp_str = self.timestamps[key]  # Obtém o timestamp armazenado pelo cliente
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")  # Converter o timestamp de volta para datetime
            random_seconds = random.randint(0,1)  # Gerar um valor aleatório de segundos entre 0 e 1
            timestamp += timedelta(seconds=random_seconds)  # Adicionar o valor aleatório de segundos ao timestamp para testes do TRY_OTHER_SERVER_OR_LATER
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")  
        else:
            timestamp = None
        message = Message("GET", key, timestamp=timestamp)
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
            print("1. INIT")
            print("2. PUT")
            print("3. GET")
            print("4. Exit")
            choice = input("Enter your choice: ")
            # Capturar IPs e portas dos servidores do teclado, função INIT
            if choice == "1":
                print("Sistema Iniciado")
                for _ in range(3):
                    ip = input("Enter server IP: ")
                    port = int(input("Enter server port: "))
                    client.add_server(ip, port)

            elif choice == "2":
                key = input("Enter key: ")
                value = input("Enter value: ")
                self.put(key, value)

            elif choice == "3":
                key = input("Enter key: ")
                self.get(key)

            elif choice == "4":
                break

            else:
                print("Opção Inválida, tente novamente.")

#Código Main
if __name__ == "__main__":
    client = Client()
    client.run()
            






