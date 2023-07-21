import socket
import threading
from datetime import datetime

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

class Server:
    def __init__(self):
        self.ip = input("Enter server IP: ")
        self.port = int(input("Enter server port: "))
        self.leader_ip = input("Enter leader IP: ")
        self.leader_port = int(input("Enter leader port: "))
        self.leader = (self.leader_ip, self.leader_port)
        self.data = {}
        self.lock = threading.Lock()

    def send_message(self, message, ip, port):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((ip, port))

        client_socket.send(message.serialize().encode())
        response = client_socket.recv(1024).decode()

        client_socket.close()

        return response

    def replicate_data(self, message):
        for server in [(self.ip, self.port), self.leader]:
            if server != (self.ip, self.port):
                self.send_message(message, server[0], server[1])

    def get(self, key, client_timestamp):
        if (self.ip, self.port) != self.leader:
            response = self.send_message(Message("GET", key, timestamp=client_timestamp), self.leader[0], self.leader[1])
            return response

        with self.lock:
            if key in self.data:
                value, timestamp = self.data[key]
                timestamp_datetime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                if timestamp_datetime >= client_timestamp:
                    return f"{value} {timestamp}"
                else:
                    return "TRY_OTHER_SERVER_OR_LATER"
            else:
                return "NULL"


    def handle_client(self, connection, address):
        client_timestamp = None  # Inicializar o timestamp do cliente para essa conexão como None

        while True:
            data = connection.recv(1024).decode()
            if not data:
                break

            message = Message.deserialize(data)
            response = ""

            if message.command == "PUT":
                if (self.ip, self.port) != self.leader:
                    response = self.send_message(message, self.leader[0], self.leader[1])
                else:
                    with self.lock:
                        if message.key in self.data:
                            self.data[message.key] = (message.value, message.timestamp)
                        else:
                            self.data[message.key] = (message.value, message.timestamp)
                    response = f"PUT_OK {message.timestamp}"

                    replication_message = Message("REPLICATION", message.key, message.value, message.timestamp)
                    self.replicate_data(replication_message)

            elif message.command == "REPLICATION":
                with self.lock:
                    if message.key in self.data:
                        self.data[message.key] = (message.value, message.timestamp)
                    else:
                        self.data[message.key] = (message.value, message.timestamp)
                response = "REPLICATION_OK"

            elif message.command == "REPLICATION_OK":
                response = f"PUT_OK {message.timestamp}"

            elif message.command == "GET":
                if message.key in self.data:
                    value, timestamp_server = self.data[message.key]
                    if client_timestamp is None or timestamp_server >= client_timestamp:
                        response = f"{value} {timestamp_server}"
                    else:
                        response = "TRY_OTHER_SERVER_OR_LATER"
                else:
                    response = "NULL"

                # Atualizar o timestamp do cliente
                client_timestamp = message.timestamp

            else:
                response = "Invalid command."

            connection.send(response.encode())

        connection.close()


    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"Server started at {self.ip}:{self.port}")

        while True:
            connection, address = server_socket.accept()
            print("New connection from", address)

            client_thread = threading.Thread(
                target=self.handle_client,
                args=(connection, address)
            )
            client_thread.start()

if __name__ == "__main__":
    server = Server()
    server.start()
