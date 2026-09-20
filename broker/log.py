
import struct

class Log:

    def __init__(self,filename):
        self.file=open(filename,"ab+")

        self.index={}

        self._build_index()

    def _build_index(self):
        self.file.seek(0)

        offset=0

        while True:
            pos=self.file.tell()

            length_bytes=self.file.read(4)

            if not length_bytes:
                break
            length=struct.unpack(">I",length_bytes)[0]

            data=self.file.read(length)

            if len(data)!=length:
                raise ValueError("Corrupted log")
            self.index[offset]=pos

            offset+=1

    def append(self,message):
       data=message.encode("utf-8")

       length=struct.pack(">I",len(data))

       self.file.seek(0,2)

       pos=self.file.tell()


       self.file.write(length)
       self.file.write(data)

       self.file.flush()

       self.index[len(self.index)]=pos


    def read(self,offset):

        if offset not in self.index:
            raise IndexError("Offset does not exist")

        pos=self.index[offset]


        self.file.seek(pos)


        length_bytes=self.file.read(4)


        length=struct.unpack(">I",length_bytes)[0]

        data=self.file.read(length)
        
        return data.decode("utf-8")
          

    def close(self):
        self.file.close()    

        

