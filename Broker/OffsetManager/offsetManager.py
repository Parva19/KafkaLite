from Logger.logger import getLogger

class OffsetManager:
    def __init__(self):
        self.offsets = {}
        # structure:
        # {
        #   "group_id": {
        #       "topic_name": {
        #           0: 10,
        #           1: 7,
        #           2: 5
        #       }
        #   }
        # }
        self.logger = getLogger("offset_manager")

    def getOffset(self, group_id=None, topic_name=None, partition_id=None):
        if group_id is None or topic_name is None or partition_id is None:
            self.logger.error("group_id, topic_name and partition_id must be provided")
            raise ValueError("group_id, topic_name and partition_id must be provided")
        
        offset = (self.offsets
                  .get(group_id, {})
                  .get(topic_name, {})
                  .get(partition_id, 0))  # default 0 = start from beginning
        
        self.logger.debug(f"Getting offset for group={group_id} topic={topic_name} partition={partition_id} -> {offset}")
        return offset

    def updateOffset(self, group_id=None, topic_name=None, partition_id=None, offset=None):
        if group_id is None or topic_name is None or partition_id is None or offset is None:
            self.logger.error("group_id, topic_name, partition_id and offset must be provided")
            raise ValueError("group_id, topic_name, partition_id and offset must be provided")
        
        (self.offsets
            .setdefault(group_id, {})
            .setdefault(topic_name, {})
            [partition_id]) = offset
        
        self.logger.debug(f"Updated offset for group={group_id} topic={topic_name} partition={partition_id} -> {offset}")

    def resetOffset(self, group_id=None, topic_name=None, partition_id=None):
        """Reset offset to 0 — allows consumer group to replay from beginning"""
        if group_id is None or topic_name is None or partition_id is None:
            self.logger.error("group_id, topic_name and partition_id must be provided")
            raise ValueError("group_id, topic_name and partition_id must be provided")
        
        self.updateOffset(group_id, topic_name, partition_id, 0)
        self.logger.info(f"Reset offset for group={group_id} topic={topic_name} partition={partition_id}")

    def getGroupStatus(self, group_id=None):
        """Returns all offsets for a group — useful for monitoring"""
        if group_id is None:
            self.logger.error("group_id must be provided")
            raise ValueError("group_id must be provided")
        
        status = self.offsets.get(group_id, {})
        self.logger.info(f"Status for group={group_id}: {status}")
        return status

    def getAllOffsets(self):
        """Returns entire offset map — useful for metrics/debugging"""
        self.logger.debug(f"All offsets: {self.offsets}")
        return self.offsets