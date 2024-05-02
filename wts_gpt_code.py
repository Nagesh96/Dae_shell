def pull_files(self):
    try:
        logging.info('Came to pull_files method')
        files_path = os.path.join('//Sdcfsvp03/data03/1792/KPI/Transactions/Monthly WTS Trade Data/', '*.txt')
        files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True) 
        logging.info('WTS files_path: '+ str(files_path))
        foundFileList = []

        for file in files:
            if file.find(datetime.datetime.today().strftime('%Y-%m'), 0, len(file)) > 0:
                # It's the current month
                foundFileList.append(file) 
        logging.info('WTS filelist:'+str(foundFileList))
        return foundFileList
    except Exception as e:
        logging.error('Error in pull_files method: {}'.format(str(e)))
        return None
