#Versão Final Código do servidor
#EP2 Sistemas Distribuidos UFABC 2023.2
#Fernando Schroder Rodrigues
#RA:11201921885

import socket
import threading

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
'''Iniciando a classe Server, um Banco de Chave valor que comunica servidores e o líder, que é definido duranta a criação do servidor'''
class Server:
    def __init__(self):
        self.ip = input("Enter server IP: ")
        self.port = int(input("Enter server port: "))
        self.leader_ip = input("Enter leader IP: ")
        self.leader_port = int(input("Enter leader port: "))
        self.leader = (self.leader_ip, self.leader_port)
        self.data = {}
        self.lock = threading.Lock() #aqui usando Locks para tratar as concorrencias das threads e não teer excpetions relacionadas a esse ponto
        self.servers_address = [10099, 10098, 10097]
        self.replication_ok_count = 0 
    '''Função send message, Usando o protocolo TCP para que seja possível a comunicação por sockets entre os componentes do sistema '''
    def send_message(self, message, ip, port):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip, port))

        client_socket.send(message.serialize().encode())
        response = client_socket.recv(1024).decode()

        client_socket.close()
        return response
    
    '''Função Replicata_data, o líder após um PUT irá chamar essa função para replicar os dados entre os servidores'''
    def replicate_data(self, message):
        for server_port in self.servers_address:
            if server_port != self.port:
                self.send_message(message, self.ip, server_port)

    '''Função de Handle_client assim como tivemos no EP1, temos uma função central que lida com as requisições e comando enviados entre os componentes do sistema'''
    def handle_client(self, connection, address):
        client_timestamp = None

        while True:
            data = connection.recv(1024).decode()
            if not data:
                break

            message = Message.deserialize(data)
            response = ""
            '''Funcão PUT: Aqui irá verificar se servidor que recebeu o request é o líder, se sim, completa o request, caso não seja encaminha para o líder '''
            if message.command == "PUT":
                if (self.ip, self.port) == self.leader:
                    print(f"Cliente {address[0]}:{address[1]} PUT key:{message.key} value:{message.value}.")
                else:
                    print(f"Encaminhando PUT key:{message.key} value:{message.value}.")
                    self.replication_ok_count = 0 

                with self.lock:
                    self.data[message.key] = (message.value, message.timestamp)
                response = f"PUT_OK {message.timestamp}"

                # Replicar os dados para outros servidores
                replication_message = Message("REPLICATION", message.key, message.value, message.timestamp)
                self.replicate_data(replication_message)
            #Função de replicação: Após os líder receber o PUT, ele replica os dados para todos os servidores do sistema, uma vez replicado, retornam um Replication_OK
            elif message.command == "REPLICATION":
                print(f"REPLICATION key:{message.key} value:{message.value} ts:{message.timestamp}.")
                with self.lock:
                    if message.key in self.data:
                        self.data[message.key] = (message.value, message.timestamp)
                    else:
                        self.data[message.key] = (message.value, message.timestamp)
                response = "REPLICATION_OK"
                response_message = Message("REPLICATION_OK", message.key, timestamp=message.timestamp)
                self.send_message(response_message, self.leader[0], self.leader[1])
            #Uma vez o líder recebendo o REPLICATION_OK o líder retorna o PUT para o cliente     
            elif message.command == "REPLICATION_OK": 
                print(f"Enviando PUT_OK ao Cliente {self.leader[0]}:{self.leader[1]} da key:{message.key} ts:{message.timestamp}.")                
                response = f"PUT_OK {message.timestamp}"
                

            #Aqui para a função GET, qualquer servidor recebe esse request, verifica se o timestamp não está divergente do que ele possui, ou que o timestamp do cliente seja maior do que ele possui
            elif message.command == "GET":
                if message.key in self.data:
                    client_timestamp = message.timestamp
                    print(client_timestamp)
                    value, timestamp_server = self.data[message.key]
                    if (message.timestamp == "None"): 
                        response = f"{value} {timestamp_server}"
                    elif (timestamp_server >= client_timestamp):
                        response = f"{value} {timestamp_server}"
                    else:
                        response = "TRY_OTHER_SERVER_OR_LATER"
                else:
                    response = "NULL"

                if response != "NULL":
                    print(f"Cliente {address[0]}:{address[1]} GET key:{message.key} ts:{message.timestamp}. Meu ts é {self.data[message.key][1]}, portanto devolvendo {self.data[message.key][0]}")

            else:
                response = "Invalid command."

            connection.send(response.encode())

        connection.close()
    #função para iniciar o servidor
    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"Server iniciado {self.ip}:{self.port}")

        while True:
            connection, address = server_socket.accept()

            client_thread = threading.Thread(
                target=self.handle_client,
                args=(connection, address)
            )
            client_thread.start()

if __name__ == "__main__":
    server = Server()
    server.start()
