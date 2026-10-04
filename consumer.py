
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


def join_group(sock,group_id,topic_name,consumer_id):
    request=f"JOIN_GROUP {group_id} {topic_name} {consumer_id}\n"
    sock.sendall(request.encode("utf-8"))
    response=recv_response(sock)
    if response.startswith("ASSIGN "):
        partition_str=response[7:]
        return [int(p) for p in partition_str.split(",")]
    else:
        raise Exception(f"Error joining group: {response}")

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


def heartbeat(sock,group_id,topic_name,consumer_id):
    request=f"HEARTBEAT {group_id} {topic_name} {consumer_id}\n"
    sock.sendall(request.encode("utf-8"))
    response=recv_response(sock)
    if not response.startswith("ASSIGN "):
        raise RuntimeError(f"Error sending heartbeat: {response}")

    partition_str=response[7:]
    return [int(p) for p in partition_str.split(",")]


def leave_group(sock,group_id,topic_name,consumer_id):
    request=f"LEAVE_GROUP {group_id} {topic_name} {consumer_id}\n"
    sock.sendall(request.encode("utf-8"))
    response=recv_response(sock)
    if response!="OK":
        raise RuntimeError(f"Error leaving group: {response}")


def read_partition(sock,topic_name,partition_id,offset):
    request=f"READ {topic_name} {partition_id} {offset}\n"
    sock.sendall(request.encode("utf-8"))
    return recv_response(sock)

def main():


    topic_name=input("Enter topic name: ")
    group_id=input("Enter consumer group id: ")
    consumer_id=input("Enter consumer id: ")
    

    sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((HOST,PORT))

    try:
        partitions=join_group(sock,group_id,topic_name,consumer_id)
        print(f"Joined group '{group_id}' and assigned partitions: {partitions}")

        last_heartbeat=time.time()

        while True:
            if time.time()-last_heartbeat>5:
                partitions=heartbeat(sock,group_id,topic_name,consumer_id)
                print(f"Heartbeat sent. Current assigned partitions: {partitions}")
                last_heartbeat=time.time()

            for partition_id in partitions:
                offset=fetch_offset(sock,group_id,topic_name,partition_id)
                print(f"Current offset for partition {partition_id}: {offset}")

                response=read_partition(sock,topic_name,partition_id,offset)

                if response.startswith("OK "):
                    msg=response[3:]
                    print(f"Partition {partition_id} - Msg received: {msg}")
                    offset+=1
                    commit_offset(sock,group_id,topic_name,partition_id,offset)
                elif response=="EMPTY":
                    print(f"Partition {partition_id} - No new messages")
                else:
                    print(f"Partition {partition_id} - Error: {response}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n Leaving group and closing connection...")
        try:
            leave_group(sock,group_id,topic_name,consumer_id)
            print(f"Left group '{group_id}'")
        except Exception as e:
            print(f"Error leaving group: {e}")
        finally:
            sock.close()
            print("Connection closed.")


if __name__=="__main__":
    main()
