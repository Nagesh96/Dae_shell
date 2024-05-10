data = [file for file in data if sunday_date in file and 'Sec_Cond' in file]

logging.info('Security Detail -get_data method data is + str(data))

for line in data:

col line.split()

datestr'.join(line.split() [0:2])

date time.strptime (datestr, '%m-%d-%y %H:%M%p')

datelist.append(date)

logging.info('Security Detail File names are is+ str(col[3]))

filelist.append(col[3])

logging.info('Security Detail -get_data method filelist is: '+ str(filelist))

combo zip(datelist, filelist)

logging.info('Security Detail line -126 combo is: '+ str(combo))

who = dict(combo) logging.info('Security Detail line -128 who is + str(who))
