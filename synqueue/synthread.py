import os
import logging
import threading
import traceback
from synqueue.synproqueue import SynProQueue


class SynThread(threading.Thread):
    def __init__(self, **kwargs):
        super().__init__()
        self.name = f"SynThread-{os.getpid()}"
        self.kfk_client = SynProQueue(**kwargs)

    def run(self) -> None:
        logging.info(f"{self.name} SynThread start... ")
        try:
            self.kfk_client.run()
        except:
            logging.error(f"{self.name} exception {traceback.format_exc()}")
        logging.info(f"{self.name} over... ")

    def close(self):
        self.kfk_client.close()
