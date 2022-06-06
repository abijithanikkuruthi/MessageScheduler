import multiprocessing
from MessageHandler import MessageHandler
from common import Config, printdebug, printheader

class MessageHandlerProcess(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.mh = MessageHandler()

    def run(self):
        self.mh.run()

if __name__ == "__main__":

    for _ in range(Config.get('sm_mh_process_count')):
        MessageHandlerProcess().start()
    
    printheader("\nMessageHandler Service started with " + str(Config.get('sm_mh_process_count')) + " processes.")