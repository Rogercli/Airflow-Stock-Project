from pathlib import Path

#path where the logs of DAG marketvol2 are stored
base_log_folder='/home/roger/airflow/logs/marketvol2'


#log analyzer takes in all log file paths and first stores them into list
#then it iterates and reads through each file and scans for ERROR or WARNING logs
def log_anaylzer(filepath):
    log_file_path=Path(filepath).rglob('*.log')
    log_file_list=list(log_file_path)
    Errors=0
    Errors_list=[]
    Warnings=0
    Warning_list=[]
    for logfile in log_file_list:
        with open(logfile,'r') as file:
            r=file.readlines()
            for line in r:
                if 'ERROR' in line:
                    Errors+=1
                    Errors_list.append(line)
                if 'WARNING' in line:
                    Warnings+=1
                    Warning_list.append(line)
    return Errors,Warnings, Errors_list, Warning_list

Errors,Warnings, Errors_list, Warning_list=log_anaylzer(base_log_folder)
print(f'Total Number of Error Logs:{Errors}')
print(f'Total Number of Warning Logs:{Warnings}')
for line in Errors_list:
    print(line)
for line in Warning_list:
    print(line) 


