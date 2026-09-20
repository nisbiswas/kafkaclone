from broker.log import Log

def main():
    print("Kafka Clone Starting.....")
    log=Log("logs/0.log")

    log.append("Hello Kafka")
    log.append("second message")
    log.append("kafka clone apppend")

    ##read log
    msg=log.read(2)
    print("current offset msg is: ", msg)



if __name__=="__main__":
    main()