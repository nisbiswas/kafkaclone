import socket

from log import Log


HOST="127.0.0.1"
PORT=9092

def main():
    log=Log("logs/0.log")

    print("Loaded Index: ",log.index)

    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

    server.bind((HOST,PORT))

    server.listen()

    print(f"Broker listening on {HOST}:{PORT}")


    while True:
        conn,address=server.accept()

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

                parts=request.split(" ",1)

                cmd=parts[0]

                try:
                    if cmd=="APPEND":
                        msg=parts[1]
                        offset=log.append(msg)
                        response=f"OK {offset}"

                        print("APPEND offset: ",offset)
                        print("Index after append: ", log.index)

                    elif cmd=="READ":
                        offset=int(parts[1])
                        msg=log.read(offset)
                        response=f"OK {msg}"

                        print("READ offset: ",offset)
                        print("Index after append: ", log.index)
                        
                    else:
                        response="ERROR unknown command"
                except Exception as e:
                    response=f"ERROR {e}"

                conn.sendall((response+"\n").encode("utf-8"))
        

        conn.close()


if __name__=="__main__":
    main()


