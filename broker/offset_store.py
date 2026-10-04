import os


class OffsetStore:

    def __init__(self,offset_dir):
        self.offset_dir=offset_dir
        os.makedirs(self.offset_dir,exist_ok=True)

    def _get_offset_file(self,group_id,topic_name,partition_id):
        return (
            f"{self.offset_dir}/{group_id}_{topic_name}_{partition_id}.txt"

        )
    def get(self,group_id,topic_name,partition_id):
        offset_file=self._get_offset_file(group_id,topic_name,partition_id)
        try:
            with open(offset_file,"r") as file:
                return int(file.read())
        except FileNotFoundError:
            return 0

    def commit(self,group_id,topic_name,partition_id,offset):
        offset_file=self._get_offset_file(group_id,topic_name,partition_id)
        with open(offset_file,"w") as file:
            file.write(str(offset))