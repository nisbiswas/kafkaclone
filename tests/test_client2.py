import socket
sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.connect(("127.0.0.1",9092))

sock.sendall(b"READ 28\n")
print(sock.recv(4096).decode())

sock.sendall(b"READ 29\n")
print(sock.recv(4096).decode())

sock.sendall(b"READ 30\n")
print(sock.recv(4096).decode())

