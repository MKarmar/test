import json
import os
import pandas as pd



def transform_data_from_json_records(**kwargs):
    date = kwargs['execution_date']
    date = date.date()
    date = str(date).replace('-','/')
    if(os.path.isfile('flattened_data/'+date+'/data.csv')):  #paths=['raw_data/2023/03/01/datalake-available-stock-changed-events-topic-2-2023-03-01-13-01-19-b7c0359b-e55f-3af9-83b4-ef036ff3f0c1']):
        raise ValueError('Data has already been processed')
    try:    
        os.makedirs('flattened_data/'+date)
    except:
        print('filepath has been created')
    file_folder = 'raw_data/'+date+'/'
    if os.path.exists(file_folder):
        file_list = os.listdir(file_folder)
    else:
        raise ValueError('No Data to process')

    full_file_paths=[]
    for file in file_list:
        full_file_paths.append(file_folder+file)

    full_file_lines = []
    for file in full_file_paths:
        with open(file, 'r') as f:
            lines = [line.rstrip() for line in f]
            full_file_lines.append(lines)
    output_path = 'flattened_data/'+date+'/data.csv'

    df = pd.DataFrame()
    for file in full_file_lines:
        for line in file:
            final = json.loads(line)
            inner_df = pd.json_normalize(final, record_path=['payload', 'changed'], meta=['type', 'timestamp', 'version', 'boundedContext'])
            if(df.empty):
                df = pd.DataFrame(inner_df)
            else:
                df = pd.concat([df, inner_df])
            #inner_df.to_csv(output_path, sep=';', mode='a', index=False, header=True)
            #inner_df.to_csv(output_path, sep=';', mode='a', index=False, header=False)

    df.to_csv(output_path, sep=';', mode='a', index=False, header=True)


