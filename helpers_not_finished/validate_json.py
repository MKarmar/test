from jsonschema import validate
from os import listdir
import json

def test_json_validity(paths = ['raw_data/2023/03/01/datalake-available-stock-changed-events-topic-2-2023-03-01-13-01-19-b7c0359b-e55f-3af9-83b4-ef036ff3f0c1'], relative_schema_path = '.schema/stock/'):

    
    schema_file_list = listdir(relative_schema_path) #list the schemas in relative schema path
    current_schema_file = max(schema_file_list) #Find newest schema for validation
    schema_file = open(relative_schema_path + str(current_schema_file)) #Open the schema file
    schema = json.load(schema_file) #Read the schema file

    '''
    Loop through all JSON objects in given file list and validate that they are correctly JSON formated
    '''
    for path in paths:
        #Build array of lines
        with open(path, 'r') as f:
                    lines = [line.rstrip() for line in f]
        #run validation for each line, that they correspond ot the json schema. A violation will cause an exception to be rissen thereby breaking the flow, causing the task to fail.
        for line in lines:
            json_object = json.loads(line)
            validate(json_object, schema)

if __name__ == "__main__":
    test_json_validity()