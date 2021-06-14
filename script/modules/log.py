#-------------------------------------------------------------------------------
# Name:        Log.py
# Purpose:     Provide a logging mechanism for the application
#
# Author:      Shawn Mason, NER GSO
#
# Created:     04/04/2019
# Copyright:   (c) masons 2019
# Licence:     Apache Version 2.0

#logging.basicConfig(format='%(asctime)s %(message)s',
#            datefmt='%m/%d/%Y %I:%M:%S %p', filename=logFile, level=logging.DEBUG)
#-------------------------------------------------------------------------------


import logging
from logging.handlers import RotatingFileHandler


class logger():
    def __init__(self, logFile, debug=True):

        self.debug_msg = ''
        self.info_msg = ''

        formatter = logging.Formatter(fmt='%(asctime)s %(message)s',datefmt='%m/%d/%Y %I:%M:%S %p')

        self.logger = logging.getLogger(logFile)
        if (debug): self.logger.setLevel(logging.DEBUG)
        else: self.logger.setLevel(logging.INFO)

        # Stream handler; console display; not required during production
##        stream_handler = logging.StreamHandler()
##        stream_handler.setFormatter(formatter)
##        self.logger.addHandler(stream_handler)

        file_handler = RotatingFileHandler(
            logFile,
            maxBytes=3000000,  # 10000000 = 1MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)


    #Create a log event for the debug logger
    #Returns: None
    def debug(self, msg):
        self.logger.debug(msg)
        self.debug_msg += msg + '<br><br>\n'

    #Create a log event for the info logger
    #Returns: None
    def info(self, msg):
        self.logger.info(msg)
        self.debug_msg += msg + '<br><br>\n'
        self.info_msg += msg + '<br><br>\n'

    #Change the log level to DEBUG mode
    #Returns: None
    def changeLogLevel(self, level='INFO'):
        if (level == 'INFO'): self.logger.setLevel(logging.INFO)
        elif (level == 'DEBUG'): self.logger.setLevel(logging.DEBUG)


#END OF CLASS

if __name__ == '__main__':
    log = logger('temp.txt')
    log.debug("some log text")
    print(log.logger)