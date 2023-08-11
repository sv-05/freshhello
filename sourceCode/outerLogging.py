import logging
import sys
class loggingCodeClass:

    def __init__(self):
        print("import working in self")
    
    def getlogger(self, name, level=logging.INFO):

        logger = logging.getLogger(name)
        logger.setLevel(level)
        # print('import working')
        if logger.handlers:
            # or else, as I found out, we keep adding handlers and duplicate messages
            pass
        else:
            ch = logging.StreamHandler(sys.stderr)
            ch.setLevel(level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger  
# logger = getlogger('my-worker')