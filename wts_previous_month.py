def pull_files(self):
    try:
        logging.info('Came to pull_files method')
        # Calculate the year and month of the previous month
        today = datetime.datetime.today()
        if today.month == 1:
            prev_month = today.replace(year=today.year - 1, month=12)
        else:
            prev_month = today.replace(month=today.month - 1)
        
        prev_month_str = prev_month.strftime('%Y-%m')

        # Construct the path to the directory containing the files to be pulled
        files_path = os.path.join('//Sdcfsvp03/data03/1792/KPI/Transactions/Monthly WTS Trade Data/', '*.txt')

        # Find all files matching the specified pattern in the directory
        files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True) 
        logging.info('WTS files_path: '+ str(files_path))

        foundFileList = []

        for file in files:
            # Extract the file name without the extension
            file_name = os.path.basename(file).split('.')[0]

            # Check if the file belongs to the previous month
            if file_name.startswith(prev_month_str):
                foundFileList.append(file) 

        logging.info('WTS filelist:'+str(foundFileList))
        return foundFileList
    except Exception as e:
        logging.error('Error in pull_files method: {}'.format(str(e)))
        return None￼Enter
