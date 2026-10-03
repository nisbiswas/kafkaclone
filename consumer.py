import os
import socket
import time

HOST="127.0.0.1"
PORT=9092

OFFSET_FILE="consumer_offset.txt"
OFFSET_DIR="consumer_offsets"




def load_offset(topic_name, partition_id):
    offset_file=f"{OFFSET_DIR}/{topic_name}_{partition_id}.txt"
    try:
        with open(offset_file,"r") as file:
           return int(file.read())

    except FileNotFoundError:
        return 0


def save_offset(offset,topic_name, partition_id):
    offset_file=f"{OFFSET_DIR}/{topic_name}_{partition_id}.txt"
    with open(offset_file,"w") as file:
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

    os.makedirs(OFFSET_DIR, exist_ok=True)
    
    topic_name=input("Enter topic name: ")
    partition_id=int(input("Enter partition id: "))
    offset=load_offset(topic_name, partition_id)

    print(f"Starting consumer for topic '{topic_name}', partition {partition_id}, starting from offset {offset}")

    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST,PORT))


    while True:
        request=f"READ {topic_name} {partition_id} {offset}\n"
        
        sock.sendall(request.encode("utf-8"))
        
        response=recv_response(sock)

        if response.startswith("OK "):
            msg=response[3:]
            print("Msg received",msg)
            offset+=1
            save_offset(offset, topic_name, partition_id)
        elif response=="EMPTY":
            print("No new messages, waiting...")
            time.sleep(1)
        else:
            print(response)
            break

    sock.close()

if __name__=="__main__":
    main()
