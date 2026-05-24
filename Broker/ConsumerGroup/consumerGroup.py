from Logger.logger import getLogger

class ConsumerGroup:
    def __init__(self, group_id=None, topic_name=None, num_partitions=None):
        if group_id is None or topic_name is None or num_partitions is None:
            raise ValueError("group_id, topic_name and num_partitions must be provided")
        
        self.group_id = group_id
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.consumers = []                  # list of consumer objects
        self.assignments = {}               # consumer_id → list of partition_ids
        self.logger = getLogger("consumer_group")

    def addConsumer(self, consumer=None):
        """Add consumer to group and rebalance partition assignments"""
        if consumer is None:
            raise ValueError("Consumer must be provided")
        
        self.consumers.append(consumer)
        self.logger.info(f"Consumer {consumer.config.consumer_id} joined group {self.group_id}")
        self.rebalance()

    def removeConsumer(self, consumer=None):
        """Remove consumer from group and rebalance"""
        if consumer is None:
            raise ValueError("Consumer must be provided")
        
        self.consumers = [c for c in self.consumers if c.config.consumer_id != consumer.config.consumer_id]
        self.assignments.pop(consumer.config.consumer_id, None)
        self.logger.info(f"Consumer {consumer.config.consumer_id} left group {self.group_id}")
        self.rebalance()

    def rebalance(self):
        """
        Assign partitions to consumers — round robin strategy.
        
        Example:
        3 consumers, 3 partitions → C1:P0, C2:P1, C3:P2
        2 consumers, 3 partitions → C1:P0,P2  C2:P1
        5 consumers, 3 partitions → C1:P0, C2:P1, C3:P2, C4:idle, C5:idle
        """
        # reset all assignments
        self.assignments = {c.config.consumer_id: [] for c in self.consumers}

        if not self.consumers:
            self.logger.info(f"No consumers in group {self.group_id}")
            return

        # round robin — assign each partition to a consumer
        for partition_id in range(self.num_partitions):
            # pick consumer by round robin
            consumer = self.consumers[partition_id % len(self.consumers)]
            self.assignments[consumer.config.consumer_id].append(partition_id)

        self.logger.info(f"Rebalanced group {self.group_id}: {self.assignments}")

    def getAssignedPartitions(self, consumer_id=None):
        """Returns list of partition_ids assigned to this consumer"""
        if consumer_id is None:
            raise ValueError("consumer_id must be provided")
        
        partitions = self.assignments.get(consumer_id, [])
        self.logger.debug(f"Consumer {consumer_id} assigned partitions: {partitions}")
        return partitions

    def getGroupStatus(self):
        """Returns full group assignment status — useful for monitoring"""
        status = {
            "group_id": self.group_id,
            "topic": self.topic_name,
            "num_consumers": len(self.consumers),
            "num_partitions": self.num_partitions,
            "assignments": self.assignments,
            "idle_consumers": [
                c.config.consumer_id 
                for c in self.consumers 
                if not self.assignments.get(c.config.consumer_id)
            ]
        }
        self.logger.info(f"Group status: {status}")
        return status