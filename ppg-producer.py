import random
import time
import json
import requests
from kafka import KafkaProducer
from time import sleep
import pandas as pd

producer = KafkaProducer(bootstrap_servers =['199.212.33.168:9092'],
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

df = pd.read_csv('E:\Data/test_data.csv')
# print(df.head())
df_len = len(df)
# print("length", df_len)
i =0 
# print("Temp data")
while i < df_len:
    acc_x=df.iloc[i,1]
    acc_y=df.iloc[i,2] 
    acc_z=df.iloc[i,3] 
    magnitude=df.iloc[i,4] 
    bvp=df.iloc[i,5]  
    temp = df.iloc[i,6]
    eda=df.iloc[i,7]
    # print(temp)
    i += 1
    obj = {"wrist_ACC_x":acc_x, "wrist_ACC_y": acc_y, "wrist_ACC_z":acc_z,
         "Magnitude": magnitude, "wrist_BVP": bvp,"wrist_TEMP":temp, "wrist_EDA": eda,
        }
    json_dump = json.dumps(obj)
    print(json_dump)
    producer.send("reco-test", value=json_dump)
    time.sleep(5)
    producer.flush()
