import logging

def setup_logger():
    logger = logging.getLogger("CRM-MCP")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger = setup_logger()

class MessageHandler:
    def __init__(self):
        self.callback_messages = []
        self.last_index = 0
        self.final_result = None
        self.metadata = {}

    def add_message(self, message):
        self.callback_messages.append(message)

    def get_messages(self) -> list:
        return self.callback_messages

    def set_final_result(self, result, metadata: dict = None):
        self.final_result = result
        if metadata:
            self.metadata = metadata

    def get_final_result(self):
        return self.final_result

    def get_metadata(self) -> dict:
        return self.metadata