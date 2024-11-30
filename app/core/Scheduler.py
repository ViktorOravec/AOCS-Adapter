import threading
import time
import os
from typing import Optional
import logging

from app.core.dataProcessing import dataProcessing


logger = logging.getLogger(__name__)
PROCESSING_DELAY = os.getenv('PROCESSING_DELAY', 3600)  # Default to 1 hour if not set
#

# Singleton class to schedule data processing
class Scheduler:
    _instance: Optional['Scheduler'] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> 'Scheduler':
        if not cls._instance:
            cls._instance = super(Scheduler, cls).__new__(cls, *args, **kwargs)
        return cls._instance
    
        
    def __init__(self) -> None:
        if not hasattr(self, 'initialized'):
            self.thread = None
            self.stop_event = threading.Event()
            self.delay = int(PROCESSING_DELAY)  # Default to 1 hour if not set
            self.initialized = True

    def _process(self) -> None:
        logger.debug("Spawned processing thread: "+ str(threading.get_ident()))
        # wait 1 seconds before starting
        time.sleep(1)
        elapsed = self.delay
        while not self.stop_event.is_set():
            if elapsed >= self.delay:
                try:
                    dataProcessing()
                    logger.info("Sleeping for " + str(self.delay) + " seconds")
                except Exception as e:
                    logger.error(f"Error processing data: {e}")
                elapsed = 0
            # else:
                # logger.info(str(threading.get_ident())  + " Sleeping for " + str(self.delay - elapsed) + " seconds")
            time.sleep(1)
            elapsed += 1
        # close
        # logger.info("Processing thread stopped")

    def start(self) -> None:
        with self._lock:
            if self.thread is None or not self.thread.is_alive():
                self.stop_event.clear()
                self.thread = threading.Thread(target=self._process)
                self.thread.start()
            else:
                logger.info("Data processing thread is already running, skipping start")

    def stop(self) -> None:
        logger.debug("Stopping processing thread")
        with self._lock:
            if self.thread is not None and self.thread.is_alive():
                self.stop_event.set()
                self.thread.join()
                self.thread = None
            else:
                logger.info("Data processing thread is not running, skipping stop")
        return

    def process_on_demand(self) -> None:
        logger.info("Processing data on demand")
        self.stop()
        self.start()
           
    
            

# Usage example:
# processor = Scheduler()
# processor.start_processing()
# processor.process_on_demand()
# processor.stop_processing()