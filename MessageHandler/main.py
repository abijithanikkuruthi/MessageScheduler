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

    pid_list = []
    for _ in range(Config.get('sm_mh_process_count')):
        p = MessageHandlerProcess()
        p.start()
        pid_list.append(p.pid)
    
    printheader("\nMessageScheduler Service started in " + str(len(pid_list)) + " processes.\nPIDs: " + str(pid_list))