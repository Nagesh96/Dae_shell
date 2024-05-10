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
      connection engine.raw_connection()
      cursor = connection.cursor()
      cursor.execute(exec_string)
      cursor.commit()
      connection.close()
      except Exception as e:
      logging.info('tarted Security Detail came to batch_log_success method exception is: '+ str(e))
      logging.exception('Exception on batch log success update')
      return
