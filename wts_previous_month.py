import os
import glob
import datetime
import logging

def pull_previous_month_files(self):
    try:
        logging.info('Came to pull_previous_month_files method')
        current_year = datetime.datetime.today().year
        current_month = datetime.datetime.today().month
        previous_month = current_month - 1 if current_month > 1 else 12
        previous_year = current_year if current_month > 1 else current_year - 1
        
        files_path = os.path.join('//Sdcfsvp03/data03/1792/KPI/Transactions/Monthly WTS Trade Data/', '*.txt')
        files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True) 
        logging.info('WTS files_path: '+ str(files_path))
        foundFileList = []

        for file in files:
            if file.find(f'{previous_year}-{previous_month:02}', 0, len(file)) > 0:
                # It's from the previous month of the current year
                foundFileList.append(file) 
        logging.info('Previous month WTS filelist:'+str(foundFileList))
        return foundFileList
    except Exception as e:
        logging.error('Error in pull_previous_month_files method: {}'.format(str(e)))
        return None
