import threading
from MessageHandler import MessageHandler
from common import Config, printheader

class MessageHandlerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.mh = MessageHandler()

    def run(self):
        self.mh.run()

if __name__ == "__main__":
    
    printheader("MessageScheduler Service")

    # Start MessageHandlerThread
    MessageHandlerThreadList = []
    for _ in range(Config.get('sm_mh_thread_count')):
        mh_thread = MessageHandlerThread()
        mh_thread.start()
        MessageHandlerThreadList.append(mh_thread)