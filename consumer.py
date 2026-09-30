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

def main():
    
    offset=load_offset()

    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST,PORT))

    request=f"READ {offset}\n"
    sock.sendall(request.encode("utf-8"))
    response=sock.recv(4096).decode("utf-8")

    if response.startswith("OK "):
        msg=response[3:]
        print("Msg received",msg)
        save_offset(offset+1)
    else:
        print(response)

    sock.close() 

if __name__=="__main__":
    main()
