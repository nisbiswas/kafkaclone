from broker.log import Log

def main():
    log=Log("logs/0.log")
    msg=log.read(0)
    print("msg received",msg)

    log.close()

if __name__=="__main__":
    main()
