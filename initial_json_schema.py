import json
import os
from genson import SchemaBuilder


def schema_builder_incremental(path = 'raw_data/2023/03/01/datalake-available-stock-changed-events-topic-2-2023-03-01-13-01-19-b7c0359b-e55f-3af9-83b4-ef036ff3f0c1'):
    builder = SchemaBuilder()
    with open(path, 'r') as f:
        lines = [line.rstrip() for line in f]

    for line in lines:
        datastore = json.loads(line)
        builder.add_object(datastore )

    schema = builder.to_schema()
    schema_list = os.listdir('.schema/stock/')
    if schema_list == []:
        f = open('.schema/stock/0.schema.json', 'w')
        f.write(str(schema).replace('\'','\"'))
        f.close
    else:
        version_of_schema = int(max(schema_list).replace('.schema/stock/', '').replace('.schema.json', ''))+1
        path = '.schema/stock/{0}.schema.json'.format(str(version_of_schema))
        f = open(path, 'w')
        f.write(str(schema).replace('\'','\"'))
        f.close

if __name__ == "__main__":
    schema_builder()