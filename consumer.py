
import socket
import time

HOST="127.0.0.1"
PORT=9092


def recv_response(sock):
    buffer=b""
    while b"\n" not in buffer:
        data=sock.recv(4096)
        if not data:
            raise ConnectionError("Broker closed the connection")
        buffer+=data
    response, _=buffer.split(b"\n",1)
    return response.decode("utf-8")

def fetch_offset(sock,group_id,topic_name,partition_id):
    request=f"FETCH_OFFSET {group_id} {topic_name} {partition_id}\n"
    sock.sendall(request.encode("utf-8"))
    response=recv_response(sock)
    if response.startswith("OK "):
        offset=int(response[3:])
        return offset
    else:
        raise Exception(f"Error fetching offset: {response}")


def commit_offset(sock,group_id,topic_name,partition_id,offset):
    request=f"COMMIT_OFFSET {group_id} {topic_name} {partition_id} {offset}\n"
    sock.sendall(request.encode("utf-8"))
    response=recv_response(sock)
    if response=="OK":
        return
    else:
        raise Exception(f"Error committing offset: {response}")

def main():


    topic_name=input("Enter topic name: ")
    partition_id=int(input("Enter partition id: "))
    group_id=input("Enter consumer group id: ")
    offset=fetch_offset(sock, group_id, topic_name, partition_id)

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
            commit_offset(sock, group_id, topic_name, partition_id, offset)
        elif response=="EMPTY":
            print("No new messages, waiting...")
            time.sleep(1)
        else:
            print(response)
            break

    sock.close()

if __name__=="__main__":
    main()
