import os
import pandas as pd
from datetime import datetime, timezone


def stock_eval(value):
    if(value >=0):
        return 'Trustworthy'
    else:
        return 'Untrustworthy' #marker that something is wrong with the data
    
def location_eval(value):
    if(len(value)==3):
        return 'Trustworthy'
    else:
        return 'Untrustworthy' #marker that something is wrong with the data
    
def timestamp_eval(value):
    try:
        value = datetime.fromisoformat(value[:-1]).astimezone(timezone.utc)
        if((datetime.fromisoformat('2020-03-02T13:00:58.081').astimezone(timezone.utc) < value) and (value< datetime.fromisoformat('2099-03-02T13:00:58.081').astimezone(timezone.utc))):
            return 'Trustworthy'
        else:
            return 'Untrustworthy' #marker that something is wrong with the data
    except:
         return 'Untrustworthy'
    
def sku_eval(value):
    if value:
        return 'Trustworthy'
    else:
        return 'Untrustworthy' #The SKU should not be null
    
def boundedContext_eval(value):
    if value:
        return 'Trustworthy'
    else:
        return 'Untrustworthy' #The SKU should not be null


def data_quality_assess(**kwargs):

    date = kwargs['execution_date']
    date = date.date()
    date = str(date).replace('-','/')
    data_path='flattened_data/'+date+'/data.csv'
    output_path='qualityassessed/'+date+'/'
    try:    
        os.makedirs(output_path)
    except:
        print('filepath has been created')

    if(os.path.isfile(data_path)):  #paths=['raw_data/2023/03/01/datalake-available-stock-changed-events-topic-2-2023-03-01-13-01-19-b7c0359b-e55f-3af9-83b4-ef036ff3f0c1']):
        df = pd.read_csv(data_path, sep=';')
    else:
        raise ValueError('No data available for this day')
    
    df['available_stock_eval'] = df['availableStockLevel'].apply(stock_eval) #Evaluate trustworthyness of stock level
    df['location_eval'] = df['location'].apply(location_eval) #Evaluate trustworthyness of location (assumption, valid locations are annotated with 3 letters - should be a lookup against data from ERP in reality)
    df['ts_eval'] = df['timestamp'].apply(timestamp_eval)
    df['sku_eval'] = df['sku'].apply(sku_eval)
    df['boundedContext_eval'] = df['boundedContext'].apply(boundedContext_eval)
    
    df.to_csv(output_path+'data.csv', sep=';', mode='a', index=False, header=True)