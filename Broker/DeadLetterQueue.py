from Logger.logger import getLogger
class DeadLetterQueue:
    def __init__(self):
        self.messages = dict()
        self.logger = getLogger("dead_letter_queue")

    def appendMessage(self,message=None,producer=None):
        self.logger.info(f"Message appended to Dead Letter Queue for producer {producer.config.producer_id}")
        if producer.config.producer_id not in self.messages:
            self.messages[producer.config.producer_id] = []
        self.messages[producer.config.producer_id].append(message)

    def getMessages(self,producer_id=None):
        if producer_id is None:
            self.logger.info(f"{producer_id} is not provided, returning empty list from Dead Letter Queue")
            return []
        self.logger.info(f"Getting messages from Dead Letter Queue for producer {producer_id}")
        messages = self.messages.get(producer_id,[])
        self.messages[producer_id] = []
        return messages

