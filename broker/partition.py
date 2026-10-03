from .log import Log

class Partition:
    def __init__(self,partition_id,log_path):
        self.partition_id=partition_id
        self.log=Log(log_path)

    def append(self,message):
        return self.log.append(message)
    
    def read(self,offset):
        return self.log.read(offset)
    
    def has_offset(self,offset):
        return offset in self.log.index



    