import os
import glob
import logging
import datetime

def pull_files(self):
    try:
        logging.info('Came to pull_files method')
        
        # Calculate the start and end of the previous month
        today = datetime.datetime.today()
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

        # Construct the path to the directory containing the files to be pulled
        files_path = os.path.join('//Sdcfsvp03/data03/1792/KPI/Transactions/Monthly WTS Trade Data/', '*.txt')

        # Find all files matching the specified pattern in the directory
        files = glob.iglob(files_path)
        logging.info('WTS files_path: ' + str(files_path))

        foundFileList = []

        # Iterate through the files and check if they were created in the previous month
        for file in files:
            file_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file))
            if first_day_of_previous_month <= file_creation_time <= last_day_of_previous_month:
                foundFileList.append(file)

        logging.info('WTS filelist: ' + str(foundFileList))
        return foundFileList
    except Exception as e:
        logging.error('Error in pull_files method: ' + str(e))
        return None
