import logging
import sys
class loggingCodeClass:
    
    def getlogger(self, name, level=logging.INFO):

        logger = logging.getLogger(name)
        logger.setLevel(level)
        if logger.handlers:
            pass
        else:
            ch = logging.StreamHandler(sys.stderr)
            ch.setLevel(level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger  