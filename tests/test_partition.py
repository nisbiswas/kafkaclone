from broker.partition import Partition


def test_partition_append_and_read():
    partition=Partition(partition_id=0,log_path="logs/test_partition-0.log")

    # Append messages to the partition
    offset1=partition.append("Message 1")
    offset2=partition.append("Message 2")
    offset3=partition.append("Message 3")

    # Read messages from the partition
    assert partition.read(offset1)=="Message 1"
    assert partition.read(offset2)=="Message 2"
    assert partition.read(offset3)=="Message 3"

def test_partition_has_offset():
    partition=Partition(partition_id=0,log_path="logs/test_partition-1.log")

    # Append messages to the partition
    offset1=partition.append("Message 1")
    offset2=partition.append("Message 2")
    offset3=partition.append("Message 3")

    # Check if the partition has the offsets
    assert partition.has_offset(offset1)==True
    assert partition.has_offset(offset2)==True
    assert partition.has_offset(offset3)==True
    assert partition.has_offset(-1)==False

def test_partition_persistance(tmp_path):
    log_path=tmp_path/"test_partition-0.log"
    partition=Partition(partition_id=0,log_path=str(log_path))

    # Append messages to the partition
    offset1=partition.append("Message 1")

    # Create a new Partition instance to simulate a restart
    new_partition=Partition(partition_id=0,log_path=str(log_path))

    # Read messages from the new partition instance
    assert new_partition.has_offset(offset1)==True
    assert new_partition.read(offset1)=="Message 1"
