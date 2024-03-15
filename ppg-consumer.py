#Find Location of Spark
import findspark
findspark.init()

#import all necessary packages
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time
import pickle 

import smtplib

# import necessary packages
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from pyspark.sql.types import *

from kafka import KafkaProducer

if __name__ == '__main__':
    
    print("Starting Spark Session")
    spark = SparkSession.builder.master("local").appName("Kafka Spark Demo").getOrCreate()
    
    # spark.sparkContext.setLogLevel("ERROR")
    
    sc=spark.sparkContext
    
    sc.setLogLevel("ERROR")
    
    ssc=StreamingContext(sc,5)
    
    print("Reading the model from pkl file")
    
    model = pickle.load(open('random_forest_model_ppg.pkl', 'rb'))
    
    print("Successfully loaded the model")
    
    producer = KafkaProducer(bootstrap_servers =['199.212.33.168:9092'],
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    # producer = KafkaProducer(bootstrap_servers='199.212.33.168:9092')
    
    
    def sendEmailHandler():
        # set up the SMTP server
        print("SEnding Email from Handler")
        s = smtplib.SMTP(host='smtp.gmail.com', port=587)
        s.starttls()
        s.login("valid_sender_email@gmail.com", 'pass1234')
        

        msg = MIMEMultipart()       # create a message

        # add in the actual person name to the message template
        message = "Abnormal behavior has detected"

        # setup the parameters of the message
        msg['From']="valid_sender_email@gmail.com"
        
        msg['To']="valid_receiver_email@gmail.com"
        msg['Subject']="Patient Health Status"

        # add in the message body
        msg.attach(MIMEText(message, 'plain'))
        
        s.send_message(msg)
        print("Email send done")
        # s.close()
        
    
        
        
        
        # records = message.collect()
        # for record in records:
        #     producer.send('output_topic', str(record))
        #     producer.flush()
        #     print("Sent")

    print("Reading from kafka topic in every 5 seconds")
    message=KafkaUtils.createDirectStream(ssc,topics=['reco-test'],kafkaParams={"metadata.broker.list":"199.212.33.168:9092"})
    
    # text=KafkaUtils.createDirectStream(ssc,topics=['output_topic'],kafkaParams={"metadata.broker.list":"199.212.33.168:9092"})
    # text.foreachRDD(handler)
    
    print("Reading from kafka done")


    data = message.map(lambda x: x[1])
    
    def functordd(rdd):
        try:  
            rdd1 = rdd.map(lambda x: json.loads(x))
            df=spark.read.json(rdd1)
            df.show()
            
            for row in df.rdd.collect():
                print("Performing prediction")
                prediction = model.predict([[row.wrist_ACC_x,row.wrist_ACC_y,row.wrist_ACC_z,row.Magnitude,row.wrist_BVP,row.wrist_TEMP,row.wrist_EDA]])
        
            output=round(prediction[0],2)
            print("Prediction Result: ", output)

            if output==0:
                print("Normal Behavior")
                # print("Sending for normal behavior")
                
                # write_to_kafka(text)
                
                # val = "Hello"
                # val.foreachRDD(lambda rdd: rdd.foreachPartition(handler))
                # handler(output)
                
                # json_dump = json.dumps(obj)
                # print(json_dump)
                # producer.send('output_topic', json_dump)
                # print("DOne sent")
                
#                 producer.send("output_topic", value=json_dump)
#                 time.sleep(5)
#                 producer.flush()
#                 print("Write successfully")
            else:
                print("Abnormal Behavior")
                # print("Sending for abnormal behavior")
                # text.foreachRDD(handler)
                # send the message via the server set up earlier.
                print("Sending Alert to for Abnormal Bahavior ")
                sendEmailHandler()
            
           
        except:
            pass
        
    data.foreachRDD(functordd)
   
    ssc.start()
    ssc.awaitTermination()
