import json
import os
import pandas as pd
from IPython.display import display

def data_quality_assess(paths=['raw_data/2023/03/01/datalake-available-stock-changed-events-topic-2-2023-03-01-13-01-19-b7c0359b-e55f-3af9-83b4-ef036ff3f0c1']):

    full_file_lines = []
    for file in paths:
        with open(file, 'r') as f:
            lines = [line.rstrip() for line in f]
            full_file_lines.append(lines)
    for file in full_file_lines:
        for line in file:
            df = pd.json_normalize(json.loads(line))
    display(df)

if __name__ == "__main__":
    data_quality_assess()