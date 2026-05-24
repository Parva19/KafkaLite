from Broker.Partition.partition import Partition


class Topic:
    def __init__(self,topic_name,num_partitions,topic_id,dead_letter_queue=None):
        self.topic_name = topic_name
        self.topic_id = topic_id
        self.partitions = [Partition(i, dead_letter_queue=dead_letter_queue) for i in range(num_partitions)]

    def getPartition(self,partition_id=None):
        if partition_id is None:
            # Return the first partition by default
            return self.partitions[0]
        return self.partitions[partition_id]