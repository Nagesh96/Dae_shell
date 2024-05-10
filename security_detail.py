"""
Security Detail insert class
Inserts Security Detail dataset to DAE SQL Server.
Called by Flask REST API implemented in api_controller.py
"""

import os
import logging
import pandas as pd
import datetime
from sqlalchemy import create_engine
import glob
import re
import ftplib
import time
import sys

class SecurityDetail:
  
    def __init__(self,batch_id,params):
        self.batch_id = batch_id
        self.params = params
        logging.getLogger().setLevel (logging.INFO)
        self.error_status = False

    def batch_log_success(self):
        logging.info('Started Security Detail came to batch_log_success method')
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)

        batch_id = str(self.batch_id)
        logging.info('Started Security Detail came to batch_log_success method batch_id: '+ str(batch_id))

        exec_string = """       
        DECLARE @curDate datetime2(7)
        SET @curDate = CURRENT_TIMESTAMP
        UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date = @curDate where i_batch_id = '"""" + batch_id + """'
        UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status 'SUCCESS' where i_batch_id = '"""" + batch_id + """'
        """
        

        try:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(exec_string)
            cursor.commit()
            connection.close()
        except Exception as e:
            logging.info('Started Security Detail came to batch_log_success method exception is: '+ str(e))
            logging.exception('Exception on batch log success update')
        return
        
    def batch_log_error(self, error_summary):
        logging.info('Started Security Detail came to batch_log_error method')
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
        batch_id = str(self.batch_id)
        logging.info('Started Security Detail came to batch_log_success method batch_id: '+ str(batch_id))
        self.error_status = True
        exec_string = """
        DECLARE @curDate datetime2(7)
        SET @curDate CURRENT_TIMESTAMP
        UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date = @curDate where i_batch_id = ?
        UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status = 'ERROR' where i_batch_id = ? 
        UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_error_msg = ? where i_batch_id = ?
        """
        try:
            connection= engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(exec_string, (batch_id, batch_id, error_summary, batch_id))
            cursor.commit()
            connection.close()
        except Exception as e:
            logging.info('Started Security Detail came to batch_log_error exception is: '+ str(e))
            logging.exception('Exception on batch log error update')
            self.batch_log_error(str(e))
        return
    def get_data(self):
        logging.info('came to get_data method Started Security Detail download process')
        filelist=[]
        t0 = time.time()
        try:
            ftp_host = os.environ.get('FTP_SERVER')
            logging.info('Security Detail- ftp_host: '+str(ftp_host))
            ftp_pass = os.environ.get('FTP_PASSWORD')
            #logging.info('Security Detail ftp_pass: '+ str(ftp_pass))
            ftp_user= os.environ.get('FTP_USERNAME')
            logging.info("Security Detail- ftp_user:" + str(ftp_user))
            f = ftplib.FTP(ftp_host)
            f.login(user=ftp_user, passwd=ftp_pass)
            f.cwd('/ftp-fund/Capacity Model/')
            #print("FTP Login Success')
            logging.info('FTP Login Success')
        except ftplib.error_perm as error:
            if error:
                #print('FTP Login Failed")
                logging.info('FTP Login Failed')
        data = []
        f.dir(data.append) 
        datelist = []
        currentyear = str(datetime.datetime.today().year)
        logging.info('Security Detail get data method currentYear' + str(currentyear))
        today = datetime.date.today()
        weekday_1 =  today.weekday() +1 % 7
        sunday_date = today - datetime.timedelta(weekday_1) 
        sunday_date = sunday_date.strftime("%Y%m%d")
        logging.info('Security Detail get data method-sunday date is' + str(sunday_date))
