import json
import os
import pandas as pd
from IPython.display import display



def transform_data_from_json_records(paths=['singlerecord.json']):
    full_file_lines = []
    for file in paths:
        with open(file, 'r') as f:
            lines = [line.rstrip() for line in f]
            full_file_lines.append(lines)
    path = 'test.csv'
    for file in full_file_lines:
        for line in file:
            final = json.loads(line)
            inner_df = pd.json_normalize(final, record_path=['payload', 'changed'], meta=['type', 'timestamp', 'version', 'boundedContext'])
            inner_df.to_csv(path, sep=';', mode='a', index=False)

    
    

if __name__ == "__main__":
    transform_data_from_json_records()


