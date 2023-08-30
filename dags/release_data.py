import pandas as pd
import os
from datetime import datetime, timezone

def remove_untrusted(df):
    df = df[df.available_stock_eval == 'Trustworthy']
    df = df[df.location_eval == 'Trustworthy']
    df = df[df.ts_eval == 'Trustworthy']
    df = df[df.sku_eval == 'Trustworthy']
    df = df[df.boundedContext_eval == 'Trustworthy']
    df = df.drop(['available_stock_eval','location_eval','ts_eval','sku_eval','boundedContext_eval'], axis=1)
    return df

def release_data(**kwargs):

    date = kwargs['execution_date']
    date = date.date()
    date = str(date).replace('-','/')
    data_path='qualityassessed/'+date+'/data.csv'
    output_path='DataRelease/'
    try:    
        os.makedirs(output_path)
    except:
        print('filepath has been created')

    if(os.path.isfile(data_path)):  #paths=['raw_data/2023/03/01/datalake-available-stock-changed-events-topic-2-2023-03-01-13-01-19-b7c0359b-e55f-3af9-83b4-ef036ff3f0c1']):
        df = pd.read_csv(data_path, sep=';')
    else:
        raise ValueError('No data available for this day')
    
    if os.path.isfile(output_path+'stock_data.csv'):
        previously_released_df = pd.read_csv(output_path+'stock_data.csv', sep=';')
        new_data_df  = pd.read_csv(data_path, sep=';')
        new_data_df = remove_untrusted(new_data_df)
        df = pd.concat([previously_released_df,new_data_df])
        df = df.sort_values('timestamp').drop_duplicates(subset='sku',keep='last')
        df.to_csv(output_path+'stock_data.csv', sep=';', mode='w', index=False, header=True)
    else:
        new_data_df  = pd.read_csv(data_path, sep=';')
        new_data_df = remove_untrusted(new_data_df)
        new_data_df = new_data_df.sort_values('timestamp').drop_duplicates(subset='sku',keep='last')
        new_data_df.to_csv(output_path+'stock_data.csv', sep=';', mode='w', index=False, header=True)


