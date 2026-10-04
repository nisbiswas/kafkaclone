import socket

from .broker import Broker

import threading




HOST="127.0.0.1"
PORT=9092

def handle_client(conn,address,broker):
    print("Client connected: ",address)
           
    buffer=b""
   
    while True:
        data=conn.recv(4096)
   
        if not data:
            break
        buffer+=data
   
        while b"\n" in buffer:
            raw_request,buffer=buffer.split(b"\n",1)
   
            request=raw_request.decode("utf-8").strip()
   
            print("Request: ",request)
   
            parts=request.split(" ")
   
            cmd=parts[0]
   
            try:
                if cmd=="APPEND":
                   
                    topic_name=parts[1]
                    partition_id=int(parts[2])
                    msg=" ".join(parts[3:])
                    offset=broker.append(topic_name,partition_id,msg)
                    response=f"OK {offset}"
   
                    print("APPEND offset: ",offset)
   
                elif cmd=="READ":
                     topic_name=parts[1]
                     partition_id=int(parts[2])
                     offset=int(parts[3])
   
                     if not broker.has_offset(topic_name,partition_id,offset):
                        response="EMPTY"

                     else:
                        msg=broker.read(topic_name,partition_id,offset)
                        response=f"OK {msg}"
   
                        print("READ offset: ",offset)

                elif cmd=="FETCH_OFFSET":
                    group_id=parts[1]
                    topic_name=parts[2]
                    partition_id=int(parts[3])
                    offset=broker.get_offset(group_id,topic_name,partition_id)
                    response=f"OK {offset}"

                elif cmd=="COMMIT_OFFSET":
                    group_id=parts[1]
                    topic_name=parts[2]
                    partition_id=int(parts[3])
                    offset=int(parts[4])
                    broker.commit_offset(group_id,topic_name,partition_id,offset)
                    response="OK"
                           
                else:
                    response="ERROR unknown command"
            except Exception as e:
                    response=f"ERROR {e}"
   
            conn.sendall((response+"\n").encode("utf-8"))
           
   
    conn.close()

    print("Client disconnected: ",address)


def main():
    broker=Broker("Logs")

    topic=broker.create_topic(topic_name="orders",num_partitions=3)

    print(f"Created topic :",topic.topic_name)
    print(f"Number of partitions :",len(topic.partitions))

    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

    server.bind((HOST,PORT))

    server.listen()

    print(f"Broker listening on {HOST}:{PORT}")
    

    while True:
        conn,address=server.accept()

        thread=threading.Thread(target=handle_client,args=(conn,address,broker))
        thread.start()
        


if __name__=="__main__":
    main()


