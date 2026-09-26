from broker.log import Log

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
    log=Log("logs/0.log")

    offset=load_offset()

    try:
        msg=log.read(offset)
        print("msg received: ",msg)
        save_offset(offset+1)
    except IndexError:
        print("No new msgs")

    log.close()

if __name__=="__main__":
    main()
