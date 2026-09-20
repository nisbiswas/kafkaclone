
import struct

class Log:

    def __init__(self,filename):
        self.file=open(filename,"ab+")

    def append(self,message):
       data=message.encode("utf-8")

       length=struct.pack(">I",len(data))

       self.file.seek(0,2)

       self.file.write(length)
       self.file.write(data)

       self.file.flush()


    def read(self,offset):
        self.file.seek(0)

        current_offset=0

        while True:
            length_bytes=self.file.read(4)

            if not length_bytes:
                raise IndexError("Offset does not exist")

            length=struct.unpack(">I",length_bytes)[0]

            data=self.file.read(length)

            if len(data)!=length:
                raise ValueError("Corrupted Log")

            
            if current_offset==offset:
                return data.decode("utf-8")
            current_offset=current_offset+1

    def close(self):
        self.file.close()    

        

