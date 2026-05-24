import threading
from Broker.Topic.topic import Topic
import asyncio 
from Broker.DeadLetterQueue import DeadLetterQueue
from Broker.OffsetManager.offsetManager import OffsetManager
from Broker.ConsumerGroup.consumerGroup import ConsumerGroup
from logging import getLogger
class Broker:
    def __init__(self):
        self.topics = {} # topic_name:Topic
        self.dead_letter_queue = DeadLetterQueue()
        self.offsetManager = OffsetManager()
        self.consumerGroups = {}
        self.logger = getLogger("broker")


    def addTopic(self,topic_name=None,num_partitions=None,topic_id=None):
        if topic_name is None or num_partitions is None or topic_id is None:
            raise ValueError("Topic name, number of partitions and topic id must be provided")
        topic = Topic(topic_name,num_partitions,topic_id,dead_letter_queue=self.dead_letter_queue)
        self.topics[topic_id] = topic
        return topic

    def registerConsumerGroup(self, group_id, topic_name,topic_id):
        """Create a new consumer group for a topic"""
        topic = self.topics[topic_id]
        group = ConsumerGroup(
            group_id=group_id,
            topic_name=topic_name,
            num_partitions=len(topic.partitions)
        )
        self.consumerGroups[group_id] = group
        return group

    def addConsumerToGroup(self, group_id, consumer):
        """Add consumer to existing group — triggers rebalance"""
        if group_id not in self.consumerGroups:
            raise ValueError(f"Group {group_id} not found")
        self.consumerGroups[group_id].addConsumer(consumer)
    
    def getTopicAndPartition(self,user_id=None,topic_id=None):
        if user_id is None:
            raise ValueError("User id must be provided")
        if topic_id is None:
            raise ValueError("Topic id must be provided")
        topic = self.topics[topic_id]
        partition_id = (int(user_id) - 1) % len(topic.partitions)
        return topic,partition_id

    async def publishMessage(self,Producer=None,message=None):
        if Producer==None:
            raise ValueError("Producer must be provided")

        topic,partition_id=self.getTopicAndPartition(Producer.config.producer_id, Producer.config.topic_id)
        offsetMessage = topic.getPartition(partition_id).appendMessage(message=message,producer=Producer)

        return offsetMessage

    async def consumeMessage(self,Consumer=None):
        if Consumer==None:
            raise ValueError("Consumer must be provided")
        group_id = Consumer.config.group_id
        #topic,partition_id=self.getTopicAndPartition(Consumer.config.consumer_id)
        topic = self.topics[Consumer.config.topic_id]

        # get assigned partitions for this consumer
        assigned_partitions = self.consumerGroups[group_id].getAssignedPartitions(
            Consumer.config.consumer_id
        )

        if not assigned_partitions:
            self.logger.info(f"Consumer {Consumer.config.consumer_id} is idle — no partitions assigned")
            return None
        messages = []
        for partition_id in assigned_partitions:
            offset = self.offsetManager.getOffset(group_id=group_id, topic_name=topic.topic_name, partition_id=partition_id)
            message=topic.getPartition(partition_id).readMessage(consumer=Consumer,offset=offset)
            if message is not None:
                self.offsetManager.updateOffset(
                    group_id=group_id,
                    topic_name=topic.topic_name,
                    partition_id=partition_id,
                    offset=offset + 1
                )
                messages.append(message)

        return messages

        