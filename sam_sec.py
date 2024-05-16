try:

logging.info('Security Detail -get_data method Came to filelist try block_step_4')

filelist = []

DVlist [file for file in files if 'DV' in file]

logging.info('Security Detail -get_data method DVlist is: '+ str(DVlist))

NTlist [file for file in files if 'NT' in file]

logging.info('Security Detail -get_data method NTlist is: '+str(NTlist)) SDlist [file for file in files if 'SD' in file]

logging.info('Security Detail -get_data method SDlist is: '+str(SDlist))

IRlist [file for file in files if 'IR' in file]

logging.info('Security Detail get_data method IRlist is: '+str(IRlist))

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

filelist [item for sub in filelist for item in sub]

logging.info('Security Detail -get_data method item for sub in filelist for item in sub- filelist is_step_10: '+ str(filelist))

final_df pd.DataFrame()

logging.info('Security Detail final_df is_step_11: '+ str(final_df))

try:

logging.info('Security Detail came to try blockkkk_step_13')

for file in filelist:

logging.info('Security Detail file name is_1+ str(file))

header = []

body = []

text = []

try:

try:

try:

logging.info('Security Detail get_data method came to 1st try block')

logging.info('Security Detail get data method came to 2nd try block')

logging.info('Security Detail get_data method with open (file, 'r', errors = 'replace') as fo: came to 3rd try block')

text.append(fo.readlines())

logging.info('Security Detail get_data method before file going to close')

fo.close()

logging.info('Security Detail get_data method - after file going to close')

logging.info('Security Detail Length of text: '+str(len(text)))

logging.info('Security Detail text LIST size is+str(sys.getsizeof(text)))

except Exception as e:

logging.info('Security Detail get_data method came to UnicodeDecodeError')

logging.info('Security Detail File is + str(file))

logging.info('Security Detail UnicodeDecodeError. + str(e))

except Exception as e:

logging.info('Security Detail- get_data method came to Exception')

logging.info('Security Detail File is + str(file)) logging.info('Security Detail error on + str(e))

except Exception as e:

logging.info('Security Detail get_data method came to 1st EXCEPT block') logging.info('Security Detail Error on opening/reading the file: '+ str(file))

logging.info('Security Detail File has issue: '+ str(e))

logging.info('Security Detail- get_data method step_1_1')

text1 text[0]

logging.info('Security Detail before delete text:')

del(text)

logging.info('Security Detail after delete text: ')

logging.info('Security Detail- get_data method step_1_2')

for i in range(0,len(text1)): if text1[i].startswith('<TR>'):

for x in text1[i+1:i+18]:

if x.startswith('<TD class=x1101>'):

header.append(x[16:-6])

else:

body.append(x[16:-6])

logging.info('Security Detail first for loop completed') logging.info('Security Detail get_data method step_14')

step_15') composite_list = [body [x:x+17] for x in range(0, len(body),17)]

logging.info('Security Detail get_data method df pd.DataFrame(composite_list, columns=header) # convert to dataframe

logging.info('Security Detail after step_16df is: '+ str(df))

logging.info('Security Detail-- get_data method step_17')
