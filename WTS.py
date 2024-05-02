

import pandas as pd
import time
import logging
import re
import datetime
from datetime import timedelta
from sqlalchemy import create_engine
import glob, os
import ftplib

class WTSInsert:

def __init__(self, batch_id, params):
    self.batch_id = batch_id
    self.params = params
    self.error_status = False
    self.engine create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True) logging.getLogger().setLevel(logging.INFO)

def pull_files(self):

    logging.info('Came to pull_files method')
    files_path = os.path.join('//Sdcfsvp03/data03/1792/KPI/Transactions/Monthly WTS Trade Data/', '*.txt')
    files = sorted (glob.iglob (files_path), key=os.path.getctime, reverse=True) 
    logging.info('WTS files_path: '+ str(files_path))
    foundFileList = []

for file in files:
    if file.find(datetime.datetime.today().strftime('%Y-%m'),0,len(file)) > 0:

    #its the current month

    foundFileList.append(file) 
logging.info('WTS filelist:'+str(foundFileList))

def main(self):
    foundFileList = self.pull_files()
    return
