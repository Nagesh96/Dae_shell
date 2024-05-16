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
