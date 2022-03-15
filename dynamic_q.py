from queue import Queue
import sys
import json
from typing import List, Any
import logging


logger = logging.getLogger(__name__)

class DynamicQueueBatching:
    """
    Takes a dataset sequence meant to be placed in a queue
    and dynamically builds each queue batch not to exceed a 
    specified size(bytes).

    :param dataset: List of objects to be batched
    :param queue: Tasks will be sent to this queue
    :param max_bytes: The size the queue batch cannot exceed, default 256kb

    Algo: Measures each object to be placed in the queue and compares
    the batch size to the max_bytes if that object were added. If the current 
    object causes the batch to exceed the max_bytes param then the batch is 
    sent to the queue and the current object is used to build the next batch.
    """
    def __init__(self, dataset: List[Any], queue=None, max_bytes: int=262144):
        self.dataset = dataset
        self.max_bytes = max_bytes
        self.msg_size = None
        self.queue = queue
        if self.queue is None:
            self.queue = Queue()
    
    def dynamic_queue_msg(self) -> None:
        """
        Starts by dynamically determining the size of the batches to get the max
        number of items in the Queue message, then populates Queue with 
        the rest of the dataset until empty.
        """
        if not isinstance(self.dataset, list): raise TypeError('Dataset is not a list')

        if not self.msg_size:
            while len(self.dataset) > 0:
                obj = self.dataset.pop(0)
                self.obj_sizes.append(sys.getsizeof(json.dumps(obj)))
                if sys.getsizeof(
                    json.dumps(self.batch)) + max(self.obj_sizes) < self.max_bytes:
                    self.batch.append(obj)
                else:
                    self.msg_size = len(self.batch)
                    self.queue.put(self.batch)
                    logger.info(f'Queue put, message length: {len(self.batch)}')
                    self.batch = []
                if len(self.dataset) == 0 and len(self.batch) > 0:
                    self.queue.put(self.batch)
        else:
            while True:
                if len(self.batch) == self.msg_size:
                    self.queue.put(self.batch)
                    self.batch = []
                elif len(self.dataset) <= self.msg_size and len(self.batch) == 0:
                    self.queue.put(self.dataset)
                    break
                self.batch.append(self.dataset.pop(0))
