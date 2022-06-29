# Import SparkSession
import io

import pandas
import boto3
import pandas as pd
# Create SparkSession

s3 = boto3.client('s3')  # again assumes boto.cfg setup, assume AWS S3
for key in s3.list_objects(Bucket='catalina-processed-data-prod',Prefix='catalina_activity/brand=Clorox')['Contents']:
    print(key['Key'])

    obj = s3.get_object(Bucket='catalina-processed-data-prod', Key=key['Key'])
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    print(df.info(verbose=True))
    df['id_raw'] = df['id_raw'].astype(int)
    print(df.info(verbose=True))

    # df.to_parquet("s3://catalina-data-validation/"+key['Key'])
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer)

    s3_resource = boto3.resource('s3')
    s3_resource.Object('catalina-data-validation', key['Key']).put(Body=parquet_buffer.getvalue())





# obj = s3c.get_object(Bucket= BUCKET_NAME , Key = KEY)
#
# emp_df = pd.read_csv(io.BytesIO(obj[‘Body’].read()),            encoding='utf8')
#
#
#
# df = pandas.read_parquet("/Users/pratikjoshi/Downloads/activity_data2022-03-11_183436.021588.parquet.snappy")
#
# print(df.info(verbose=True))
#
# df['id_raw'] = df['id_raw'].astype(int)
#
# print(df.info(verbose=True))



