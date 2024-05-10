def batch_log_error(self, error_summary):

logging.info('Started Security Detail came to batch_log_error method')

engine create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)

batch_id= str(self.batch_id)

logging.info('Started Security Detail came to batch_log_success method batch_id: '+ str(batch_id))

self.error_status True

exec_string =

DECLARE @curDate datetime2(7)

SET @curDate CURRENT_TIMESTAMP

UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date = @curDate where i_batch_id = ?

UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status = 'ERROR' where i_batch_id = ? UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_error_msg = ? where i_batch_id = ?

try:

connection engine.raw_connection()

cursor connection.cursor()

cursor.execute(exec_string, (batch_id, batch_id, error_summary, batch_id))

cursor.commit()

connection.close()

except Exception as e:

logging.info('Started Security Detail came to batch_log_error exception is: '+ str(e))

logging.exception('Exception on batch log error update')

self.batch_log_error(str(e))
