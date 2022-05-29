import multiprocessing
from constants import *

class CollectorProcess(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        pass

class Collector(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
    
    def run(self):
        for _ in range(COLLECTOR_PROCESS_COUNT):
            CollectorProcess().start()
            