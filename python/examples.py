import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

data_nested_struct = [
    {
        'id': '001',
        'company': 'XYZ pvt ltd',
        'location': 'London',
        'info': {
            'a' : 10,
            'b' : "hello"
        }
    },
    {
        'id': '002',
        'company': 'PQR Associates',
        'location': 'Abu Dhabi',
        'info': {
            'a' : 12,
            'b' : "bye"
        }
    }
]

df_nested_struct = pd.json_normalize(data_nested_struct)
print(df_nested_struct)
df_nested_struct.to_parquet("/home/data/nested_struct.parquet")


data_nested_collecton_struct = [
    {
        'id': '001',
        'company': 'XYZ pvt ltd',
        'location': 'London',
        'info': [{
            'a' : 10,
            'b' : "hello"
        },{
            'a' : 20,
            'b' : "hi"
        }]
    },
    {
        'id': '002',
        'company': 'PQR Associates',
        'location': 'Abu Dhabi',
        'info': [{
            'a' : 12,
            'b' : "bye"
        }]
    }
]

df_nested_collecton_struct = pd.json_normalize(data_nested_collecton_struct)
print(df_nested_collecton_struct)
df_nested_collecton_struct.to_parquet("/home/data/nested_collecton_struct.parquet")
df_nested_collecton_struct.to_parquet("/home/data/nested_collecton_struct_compliant.parquet", use_compliant_nested_type=True)
