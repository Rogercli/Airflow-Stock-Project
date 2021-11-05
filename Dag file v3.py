import os
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta, date
import yfinance as yf
import pandas as pd

default_args = {'start_date': datetime(2021, 11, 1), 
                'execution_timeout':timedelta(seconds=5)}

stock_dag=DAG(
    'marketvol2',
    default_args=default_args,
    description='Stock_dag',
    schedule_interval='45 16 * * 1-5',
    catchup=True
)

# method one using strtime
# _date=date.today()
# '''strfttime converts datetime object into desired format'''
# today = _date.strftime("%d/%m/%Y")
# '''mrkdir -p will create parent directory if is does not exist'''
# t0=BashOperator(
#     task_id='create_dir',
#     bash_command=f'mkdir -p /temp/data/{today}',
#     dag=stock_dag
# )


# method three using date.today() without str conversion
# today=date.today()
# t0=BashOperator(
#     task_id='create_dir',
#     bash_command=f'mkdir -p /temp/data/{today}',
#     dag=stock_dag
# )


dest_dir= '/home/roger/SB/Airflow_stock_project/temp2/data/{{ds_nodash}}'
dest_sub_dir='/home/roger/SB/Airflow_stock_project/temp2/data/{{ds_nodash}}/'
temp_sub_dir='mv /home/roger/{{ds}}'


#used airflow variables instead of python datetime objects in functions since
#times in bash/python operators will not conflict in case "catchup=True" to run previous dates data 
def download_data(symbol,**context):
    start_date=str(context['ds'])
    end_date=str(context['next_ds'])
    tsla_df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    tsla_df.to_csv(f"{start_date}{symbol}_data.csv", header=True)



def query(**context):
    filepath='/home/roger/SB/Airflow_stock_project/temp2/data/'
    # day_withdath= date.today()
    # day=day_withdath.strftime("%Y%m%d")
    day=str(context['ds'])
    day_nodash=str(context['ds_nodash'])
    tsla_df=pd.read_csv(filepath+day_nodash+'/'+day+'TSLA_data.csv')
    aapl_df=pd.read_csv(filepath+day_nodash+'/'+day+'AAPL_data.csv')
    t=tsla_df['Close'].describe()
    a=aapl_df['Close'].describe()
    print(f'Apple Closing Price Stats({day}):')
    print(a)
    print('\n\n')
    print(f'Tesla Closing Price Stats({day}):')
    print(t)




t0=BashOperator(
    task_id='create_dir',
    bash_command=f'mkdir -p {dest_dir}',
    dag=stock_dag
)

t1=PythonOperator(
    task_id='TSLA_Data',
    python_callable=download_data,
    op_kwargs={'symbol':'TSLA'},
    provide_context=True,
    dag=stock_dag)

t2=PythonOperator(
    task_id='AAPL_Data',
    python_callable=download_data,
    op_kwargs={'symbol':'AAPL'},
    provide_context=True,
    dag=stock_dag)

t3=BashOperator(
    task_id='move_TSLA_data',
    bash_command= f'{temp_sub_dir}TSLA_data.csv {dest_sub_dir}',
    dag=stock_dag)

t4=BashOperator(
    task_id='move_AAPL_data',
    bash_command= f'{temp_sub_dir}AAPL_data.csv {dest_sub_dir}',
    dag=stock_dag)

t5=PythonOperator(
    task_id='python_query',
    python_callable=query,
    provide_context=True,
    dag=stock_dag)

t0>>[t1,t2]
t1>>t3
t2>>t4
[t3,t4]>>t5
