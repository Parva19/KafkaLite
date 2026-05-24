from Producer.config import ProducerConfig
import time
from concurrent.futures import ThreadPoolExecutor
from Broker.broker import Broker
from Logger.logger import getLogger
import asyncio 


class Producer:
    def __init__(self,config,broker=None):
        self.config = config
        self.broker = broker
        self.logger = getLogger("producer")

    def generate_message(self):
        self.logger.info(f"Generating message for {self.config.producer_id}")
        message = f"producer {self.config.producer_id} generate the messsage id- {self.config.message_id}"
        self.config.message_id+=1
        return message
    
    async def sendMessage(self,message=None):
        if self.broker:
            self.logger.debug(f"sending message for {self.config.producer_id}")
            self.logger.info(f"sending message for {self.config.producer_id}")
            await self.broker.publishMessage(Producer=self,message=message)
        else:
            self.logger.info(f"Sending message for {self.config.producer_id}")
            self.logger.debug(f"Message for {self.config.producer_id}: {message}")

    async def run(self):
        while True:
            message = self.generate_message()
            await self.sendMessage(message)
            await asyncio.sleep(self.config.sleepTime)
            
async def startGeneratingMessage(producer_list=None,broker=None):
    tasks = []
    for ithProducer in producer_list:
        p = Producer(ithProducer, broker)
        tasks.append(p.run())  # collect coroutines
    
    await asyncio.gather(*tasks)  # run all simultaneously

if __name__ == "__main__":
    #create a broker instance and pass it to producers
    #make a main function whre we create a broker instance and pass it to producers
    asyncio.run(startGeneratingMessage(Producer_List))