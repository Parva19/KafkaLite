# logger.py  (put this at root level of project)

import logging
import os
from datetime import date

def getLogger(name):
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger  # avoid duplicate handlers
    
    logger.setLevel(logging.DEBUG)
    
    # create logs folder if not exists
    os.makedirs("logs", exist_ok=True)
    
    # file handler
    fileHandler = logging.FileHandler(f"logs/kafkalite_{date.today()}.log")
    fileHandler.setLevel(logging.DEBUG)
    
    # console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    
    # format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fileHandler.setFormatter(formatter)
    consoleHandler.setFormatter(formatter)
    
    logger.addHandler(fileHandler)
    logger.addHandler(consoleHandler)
    
    return logger