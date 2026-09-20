from broker.log import Log

def main():
    print("Kafka Clone Starting.....")
    log=Log("logs/0.log")

    # log.append("Hello Kafka")
    # log.append("second message")
    # log.append("kafka clone apppend")
    # log.append("added indexing")

    ##read log
    for offset in range(16):
        print(offset,"->",log.read(offset))

    print("Index: ",log.index)




if __name__=="__main__":
    main()