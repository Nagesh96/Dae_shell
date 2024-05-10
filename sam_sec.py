def get_data(self):

logging.info('came to get_data method Started Security Detail download process")

filelist=[]

to = time.time()

try:

ftp_hostos.environ.get('FTP_SERVER')

logging.info('Security Detail- ftp_host: '+str(ftp_host))

ftp_pass os.environ.get('FTP_PASSWORD')

#logging.info('Security Detail ftp_pass: '+ str(ftp_pass))

ftp_user= os.environ.get('FTP_USERNAME')

logging.info("Security Detail- ftp_user: + str(ftp_user))

foftplib.FTP(ftp_host)

f.login(user-ftp_user, passwd=ftp_pass)

f.cwd('/ftp-fund/Capacity Model/)

#print("FTP Login Success')

logging.info('FTP Login Success')

except ftplib.error_perm as error:

if error:

#print('FTP Login Failed")

logging.info('FTP Login Failed')

data = []

f.dir(data.append) datelist = []

currentyear str(datetime.datetime.today().year)

logging.info('Security Detail get data method currentYear+ str(currentYear))

today datetime.date.today()

weekday 1 today.weekday() +1%7

sunday date today datetime.timedelta(weekday_1) sunday date sunday_date.strftime("%d")

logging.info('Security Detail get data method-sunday date is str(sunday_date))
