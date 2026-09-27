import socket

sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)

sock.connect(("127.0.0.1",9092))

## write client
sock.sendall(b"APPEND Hello TCP Kafka\n")
## Read CLient
sock.sendall(b"READ 28")
response=sock.recv(4096)

print(response.decode())

sock.close()

