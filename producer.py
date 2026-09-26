from broker.log import Log


def main():
    log=Log("logs/0.log")
    msg=input("Enter msg: ")
    offset=log.append(msg)
    print("Msg sent")
    print("offset: ",offset)

    log.close()


if __name__=="__main__":
    main()