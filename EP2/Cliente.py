import socket
import threading
from datetime import datetime
import random

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
                timestamp = datetime.now().timestamp()
                message = Message("PUT", key, value, timestamp)
                response = self.send_message(message)
                print(response)

            elif choice == "2":
                key = input("Enter key: ")
                timestamp = 0  # Initial timestamp is 0
                message = Message("GET", key, timestamp=timestamp)
                response = self.send_message(message)
                print(response)

            elif choice == "3":
                break

            else:
                print("Invalid choice. Try again.")

if __name__ == "__main__":
    client = Client()

    # Capturar IPs e portas dos servidores do teclado
    for _ in range(3):
        ip = input("Enter server IP: ")
        port = int(input("Enter server port: "))
        client.add_server(ip, port)

    client.run()
