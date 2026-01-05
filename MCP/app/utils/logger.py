import logging
import sys
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger():
    logger = logging.getLogger("CRM-MCP")
    logger.propagate = False 
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()

    log_dir = os.path.join(os.getenv("ROOT_PATH", "."), "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, "MCP.log")
    
    fh = TimedRotatingFileHandler(filename=log_file_path, when="midnight", interval=1, backupCount=7)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

    ch = logging.StreamHandler(stream=sys.stderr)
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)

    return logger

logger = setup_logger()