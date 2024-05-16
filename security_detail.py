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
        
        data = [file for file in data if sunday_date in file and 'Sec_Cond' in file]
        logging.info('Security Detail -get_data method data is' + str(data))

        for line in data:
            col = line.split()
            datestr = ' '.join(line.split() [0:2])
            date = time.strptime (datestr, '%m-%d-%y %H:%M%p')
            datelist.append(date)
            logging.info('Security Detail File names are is' + str(col[3]))
            filelist.append(col[3])
        logging.info('Security Detail -get_data method filelist is: '+ str(filelist))
        combo = zip(datelist, filelist)
        who = dict(combo) 
        try:
            logging.info('Security Detail Came to try block')
            for key in who:
                f.retrbinary('RETR %s' % who [key], open (who [key], 'wb').write)
            logging.info('Security Detail line -138 file going to before quit')
            f.quit()
            logging.info('Security Detail file going to after quit')
            if f.close:
                logging.info('Security Detail if file closed')
            else:
                logging.info('Security Detail else file is NOT closed')
            logging.info('Security Detail file completed last line')

            logging.info('Security Detail Came to try block_step_1')
            files_path= os.path.join(os.getcwd(), sunday_date + '*Sec_Cond*.xls')
            logging.info('Security Detail -get_data method line 151 files_path is_step_2: '+ str(files_path))
            files = sorted (
                    glob.iglob(files_path), key=os.path.getctime, reverse=True)
            logging.info('Security Detail -get_data method line-154 files is_step_3: '+ str(files)) 
        except Exception as e:
            logging.info('Security Detail Came to except block')
            logging.info('Security Detail exception is: '+ str(e))
            logging.exception('Exception on downloading Security Detail files')
            self.batch_log_error(str(e))
            logging.exception('Security Detail Came to except block going to RETURN something')
            return
        try:
            logging.info('Security Detail -get_data method Came to filelist try block_step_4')
            filelist = []
            DVlist = [file for file in files if 'DV' in file]
            logging.info('Security Detail -get_data method DVlist is: '+ str(DVlist))
            NTlist = [file for file in files if 'NT' in file]
            logging.info('Security Detail -get_data method NTlist is: '+ str(NTlist)) 
            SDlist = [file for file in files if 'SD' in file]
            logging.info('Security Detail -get_data method SDlist is: '+ str(SDlist))
            IRlist = [file for file in files if 'IR' in file]
            logging.info('Security Detail get_data method IRlist is: '+ str(IRlist))
            EMlist = [file for file in files if 'EM' in file]
            logging.info('Security Detail -get_data method EMlist is: '+ str(EMlist))

            filelist.append(DVlist[0:2])
            logging.info('Security Detail -get_data method DVlist filelist is_step_5: '+ str(filelist))
            filelist.append(NTlist[0:2])
            logging.info('Security Detail -get_data method NTlist filelist is_step_6: '+ str(filelist))
            filelist.append(SDlist[0:2 ])
            logging.info('Security Detail -get_data method SDlist filelist is_step_7: '+ str(filelist))
            filelist.append(IRlist[0:2])
            logging.info('Security Detail -get_data method IRlist filelist is_step_8: '+ str(filelist))
            filelist.append(EMlist[0:2])
            logging.info('Security Detail -get_data method- EMlist filelist is_step_9: '+ str(filelist))

            filelist = [item for sub in filelist for item in sub]
            logging.info('Security Detail -get_data method item for sub in filelist for item in sub- filelist is_step_10: '+ str(filelist))

            final_df = pd.DataFrame()
            logging.info('Security Detail final_df is_step_11: '+ str(final_df))
            try:
                logging.info('Security Detail came to try blockkkk_step_13')
                for file in filelist:
                    logging.info('Security Detail file name is_1 :'+ str(file))
                    header = []
                    body = []
                    text = []
                    try:
                        logging.info('Security Detail get_data method came to 1st try block')
                        try:
                            logging.info('Security Detail get data method came to 2nd try block')
                            try:
                                logging.info('Security Detail get_data method came to 3rd try block')
                                with open (file, 'r', errors = 'replace') as fo:
                                    text.append(fo.readlines())
                                logging.info('Security Detail get_data method before file going to close')
                                fo.close()
                                logging.info('Security Detail get_data method - after file going to close')
                                logging.info('Security Detail Length of text: '+str(len(text)))
                                logging.info('Security Detail text LIST size is :'+str(sys.getsizeof(text)))
                            except Exception as e:
                                logging.info('Security Detail get_data method came to UnicodeDecodeError')
                                logging.info('Security Detail File is '+ str(file))
                                logging.info('Security Detail UnicodeDecodeError:' + str(e))
                        except Exception as e:
                            logging.info('Security Detail- get_data method came to Exception')
                            logging.info('Security Detail File is '+ str(file)) 
                            logging.info('Security Detail error on '+ str(e))
                    except Exception as e:
                        logging.info('Security Detail get_data method came to 1st EXCEPT block') 
                        logging.info('Security Detail Error on opening/reading the file: '+ str(file))
                        logging.info('Security Detail File has issue: '+ str(e))
                    logging.info('Security Detail- get_data method step_1_1')
                    text1 = text[0]
                    logging.info('Security Detail before delete text:')
                    del(text)
                    logging.info('Security Detail after delete text: ')
                    logging.info('Security Detail- get_data method step_1_2')
                    for i in range(0,len(text1)):
                        if text1[i].startswith('<TR>'):
                            for x in text1[i+1:i+18]:
                                if x.startswith('<TD class=x1101>'):
                                    header.append(x[16:-6])
                                else:
                                    body.append(x[16:-6])
                    logging.info('Security Detail first for loop completed') 
                    logging.info('Security Detail get_data method step_14')
                    composite_list = [body [x:x+17] for x in range(0, len(body),17)]
                    logging.info('Security Detail get_data method step-15')

                    df = pd.DataFrame(composite_list, columns=header) # convert to dataframe
                    logging.info('Security Detail after step_16 df is: '+ str(df))
                    logging.info('Security Detail-- get_data method step_17')

                    df = df [['FND_CU', 'FND_SU', 'FND_SD', 'FND_TC', 'FND_U0']]
                    logging.info('Security Detail get_data method step_18')
                    logging.info('Security Detail df is: '+ str(df))
                    logging.info('Security Detail file[33:35] is:'+str(file[33:35]))
                    logging.info('Security Detail-- get_data method step_20')
                    df['userbank'] = file [33:35]
                    df = df.drop_duplicates (keep='first')
                    final_df = final_df.append(df)
                    logging.info('Security Detail-- get_data method step_21') 
                    logging.info('Security Detail before final_df is'+ str(final_df))
                    logging.info('Security Detail-- get_data method step_22')
                    logging.info('Security Detail get_data method df is: '+ str(df))
                    logging.info('Security Detail get_data method step_23')
                    logging.info('Security Detail get_data method step_24')
            except Exception as e:
                logging.info('Security Detail try blockk exception is '+ str(e))
                logging.info('Security Detail Came to get_data filelist except block')
                logging.info('Security Detail-- get_data method step_25')
                final_df = final_df.drop_duplicates (keep='first', subset=['FND_CU', 'FND_SU', 'userbank'])
                logging.info('Security Detail after step_8-- final_df is'+ str(final_df))
                logging.info('Security Detail-- get_data method step_26') 
                logging.info('Completed Security Detail download and parsing')
                logging.info('Security Detail-- going to return finally')
                return final_df
        except Exception as e:
            logging.info('Security Detail exception is: '+ str(e))
            logging.info('Security Detail Came to get data filelist except block')
            logging.exception('Exception occurred on downloading and parsing Security Detail')
            self.batch_log_error(str(e))
