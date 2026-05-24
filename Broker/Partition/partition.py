import threading
from Logger.logger import getLogger
from Broker.DeadLetterQueue import DeadLetterQueue
import time
class Partition:
    def __init__(self,partition_id,dead_letter_queue=None):
        self.lock = threading.Lock()
        self.messages = list()
        self.partition_id = partition_id
        self.messageLimit = 100
        self.logger = getLogger("partition")
        self.dead_letter_queue = dead_letter_queue

    def appendMessage(self,message=None,producer=None):   
        self.logger.info(f"Producer {producer.config.producer_id} appending the message in partiotion {self.partition_id}")
        retryCount = 0
        self.lock.acquire()
        while len(self.messages) >= self.messageLimit and retryCount<3:
            time.sleep(1)  # wait for some time before retrying
            retryCount += 1
        if retryCount==3:
            self.logger.error(f"Producer {producer.config.producer_id} failed to append message in partition {self.partition_id}, threfore pushing message in Dead Letter Queue")
            self.dead_letter_queue.appendMessage(message=message,producer=producer)
            self.lock.release()
            return -1 #indicate failure to append message

        
        self.messages.append(message)
        self.lock.release()
        return len(self.messages)#return offset

    
    def readMessage(self,consumer=None,offset=None):
        self.lock.acquire()
        self.logger.info(f"Consumer {consumer.config.consumer_id} reading the message in partiotion {self.partition_id}")
        """if offset is not None:
            consumer.config.offset = offset
        if consumer.config.offset >= len(self.messages):
            self.lock.release()
            return None  # no new messages"""
        if offset >= len(self.messages):
            self.lock.release()
            return None  # no new messages
        message = self.messages[offset]
        consumer.config.offset += 1
        self.lock.release()
        return message #return the message and increase the offset counter

    def readDeadLetterQueueMessages(self,producer=None):
        if self.dead_letter_queue:
            self.logger.info(f"Reading messaged from DeadLetterQueue for producer {producer.config.producer_id}")
            deadLetterMessages = self.dead_letter_queue.getMessages(producer_id=producer.config.producer_id)
        else:
            self.logger.info(f"Consumer {producer.config.producer_id} tried to read messages from Dead Letter Queue for partition {self.partition_id}, but no Dead Letter Queue is configured")
            deadLetterMessages = []
        
        for message in deadLetterMessages:
            self.logger.debug(f"Reprocessing message from Dead Letter Queue for producer {producer.config.producer_id} in partition {self.partition_id}")
            self.appendMessage(message=message,producer=producer)
 