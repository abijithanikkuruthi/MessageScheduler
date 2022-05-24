import multiprocessing
import threading
from MessageHandler import MessageHandler
from common import Config, printdebug, printheader

class MessageHandlerProcess(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)

    def run(self):
        printdebug(f'MessageHandlerProcess.run(): Starting MessageHandlerProcess {threading.get_native_id()}')
        for _ in range(Config.get('sm_mh_thread_count')):
            threading.Thread(target=MessageHandler().run).start()

if __name__ == "__main__":
    
    printheader("MessageScheduler Service")

    for _ in range(Config.get('sm_mh_process_count')):
        MessageHandlerProcess().start()