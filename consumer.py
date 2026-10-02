import socket

HOST="127.0.0.1"
PORT=9092

OFFSET_FILE="consumer_offset.txt"

def load_offset():
    try:
        with open(OFFSET_FILE,"r") as file:
           return int(file.read())

    except FileNotFoundError:
        return 0


def save_offset(offset):
    with open(OFFSET_FILE,"w") as file:
        file.write(str(offset))

def recv_response(sock):
    buffer=b""
    while b"\n" not in buffer:
        data=sock.recv(4096)
        if not data:
            raise ConnectionError("Broker closed the connection")
        buffer+=data
    response, _=buffer.split(b"\n",1)
    return response.decode("utf-8")

def main():
    
    offset=load_offset()

    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST,PORT))

    while True:
        request=f"READ {offset}\n"
        
        sock.sendall(request.encode("utf-8"))
        
        response=recv_response(sock)

        if response.startswith("OK "):
            msg=response[3:]
            print("Msg received",msg)
            offset+=1
            save_offset(offset)
        else:
            print(response)
            break

    sock.close()

if __name__=="__main__":
    main()
