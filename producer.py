import socket

HOST="127.0.0.1"
PORT=9092


def main():

    msg=input("Enter msg: ")
    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST,PORT))
    request=f"APPEND {msg}\n"
    sock.sendall(request.encode("utf-8"))
    response=sock.recv(4096)
    print("Broker: ",response.decode("utf-8"))
    sock.close()
        

if __name__=="__main__":
    main()