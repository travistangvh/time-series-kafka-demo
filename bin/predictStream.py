import argparse
import wfdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import numpy as np
from utils import evaluate, load_dataset
import torch
from torch import nn
from models import MyCNN
from utils import get_global_config, compute_batch_accuracy, acked, get_producer_config, get_consumer_config, build_spark_session, get_waveform_path, create_batch, get_arr, get_base_time, get_ending_time,get_record,run_model,run_model_dummy
from pyspark.sql.functions import explode, split, from_json, to_json, col, struct, trim,expr
import pyspark.pandas as ps
import mysql.connector
import datetime
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, ArrayType
from pyspark.sql.functions import lit

cfg = get_global_config()
cnx = mysql.connector.connect(user='root', 
                              password='mauFJcuf5dhRMQrjj',
                              host='172.18.0.8', 
                              database='mydb')

# Define a function to write each batch of streaming data to MySQL
def write_to_mysql(batch_df, batch_id):

    # Wait until enough data points has arrived.
    import time 
    start_time = time.time()
    print(f"The number of rows of data: {len(batch_df.head(1))}")
    # Set the timeout for waiting for data
    timeout = 60  # Wait for 60 seconds

    # while len(batch_df.head(1)) < 120:  # Wait until at least 10 rows have arrived
    #     time.sleep(1)
    #     if time.time() - start_time > timeout:
    #         # If the timeout is exceeded, break out of the loop and return
    #         break
    #     time.sleep(1)  # Wait for 1 second before checking again
    #     print("Not enough data has arrived.")

    # if len(batch_df.head(1)) >= 120:
    print("Enough data has arrived!")

    batch_df_collected = batch_df.collect()
    rows = list(batch_df_collected)
    print(rows)
    
    # Retrieve the "value" column from batch_df
    batch_df = batch_df.select("key", "value")

    # select the value columns and convert it to a pandas dataframe
    # batch_df = batch_df.select("value").toPandas()

    # print(batch_df)

    # what's inside the value: ("CAST(value as STRING) AS a", "timestamp", "1 as ID")
    value_schema = StructType([
        StructField("a", StringType()),
        StructField("ID", IntegerType()),
        StructField("structs", StructType())
    ])

    # Convert the "structs" column from a json string to a column of structs
    struct_schema = StructType([
        StructField("HR", ArrayType(FloatType())), # this is NOT a float type or an INT type! 
        StructField("RESP", ArrayType(FloatType())),
        StructField("PULSE", ArrayType(FloatType())),
        StructField("etco2", ArrayType(FloatType())),
        StructField("CVP", ArrayType(FloatType())),
        StructField("AWRR", ArrayType(FloatType())),
        StructField("NBP_Mean", ArrayType(FloatType())),
        StructField("NBP_Dias", ArrayType(FloatType())),
        StructField("NBP_Sys", ArrayType(FloatType()))
        ])

    batch_df_collected = batch_df.select("value").collect()
    rows = list(batch_df_collected)
    print(rows)

    parsed_df = batch_df \
    .select(from_json(col("value").cast("string"), struct_schema).alias("structs")) 


    parsed_df_collected = parsed_df.collect()
    rows = list(parsed_df_collected)
    print(rows)


    # parsed_df = parsed_df.select("structs.a", "parsed_value.id", "parsed_value.structs") \
    # .withColumn("structs_a", from_json(col("structs").cast("string"), struct_schema))
    
    # parsed_df_collected = parsed_df.collect()
    # rows = list(parsed_df_collected)
    # print(rows)

    parsed_df = parsed_df.select("structs.HR", "structs.RESP", "structs.PULSE", "structs.etco2", "structs.CVP", "structs.AWRR", "structs.NBP_Mean", "structs.NBP_Dias", "structs.NBP_Sys")


    # parsed_df = parsed_df \
    # .select("HR", col("structs.HR")) \
    # .withColumn("RESP", expr("trim(BOTH '\"' FROM structs_a.RESP)").cast(FloatType())) \
    # .withColumn("PULSE", expr("trim(BOTH '\"' FROM structs_a.PULSE)").cast(FloatType())) 
        
    # .withColumn("field1", trim(parsed_df["structs_a.field1"],'"').cast(IntegerType())) \
    # .withColumn("field2", trim(parsed_df["structs_a.field2"],'"').cast(IntegerType())) \
    # .withColumn("field3", trim(parsed_df["structs_a.field3"],'"').cast(IntegerType()))

    print('yoohoo')
    print(parsed_df)
    parsed_df_collected = parsed_df.select("HR","RESP").collect()
    rows = list(parsed_df_collected)
    print(rows)

    # # Convert the JSON column to a struct column
    # struct_col = from_json(batch_df["structs"], struct_schema)

    # # Select the struct column and convert it to a numpy array
    # numpy_array = np.array(batch_df.select(struct_col).collect())

    # print(numpy_array)

    cursor = cnx.cursor()

    # Call ML

    # Convert the batch DataFrame to a list of tuples
    data = [tuple(row) for row in parsed_df_collected]

    # Construct the SQL query to insert the data into MySQL
    query = "INSERT INTO mytable (col1, col2) VALUES (3484, 3092)"
    # query = "INSERT INTO mytable (col1, col2) VALUES (%s, %s)"

    # Insert the data into MySQL using a prepared statement
    cursor.executemany(query, data)
    cnx.commit()


def main():
    spark = build_spark_session()

    """Get arguments from command line"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model-call-topic', 
                        type=str,
                        help='Name of the Kafka topic to receive machine learning call.')

    parser.add_argument('--model-response-topic', 
                        type=str,
                        help='Name of the Kafka topic to send the response to the call.')
    
    args = parser.parse_args()

    """Create producer and consumer and interact with kafka"""
    consumer_conf = get_consumer_config()
    producer_conf = get_producer_config()
    producer_conf['kafka.bootstrap.servers'] = producer_conf['bootstrap.servers']
    del producer_conf['bootstrap.servers']

    def msg_process(server, topic):
        """Create a streaming dataframe that takes in values of messages received, 
        together with the current timestamp.
        Then, print them out.
        Then, process the message in batch
        Reference link: https://medium.com/@aman.parmar17/handling-real-time-kafka-data-streams-using-pyspark-8b6616a3a084"""

        print(f"Reading from {topic}!")
        df = (spark.
                readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", server)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .load()
            )
        
        # Select the value and timestamp (the message is received)
        #  DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint, timestamp: timestamp, timestampType: int]
        df = df.selectExpr("CAST(key as STRING)" ,"CAST(value as STRING)")

        # df_collected = df.select("value").collect()
        # rows = list(df_collected)
        # print(rows)

        # Write a UDF that applies data to one mini batch at one time
        # The mini batch is a list of tuples, where each tuple is a message and its timestamp.
        # The model needs to be called to store the data
        model = torch.load(cfg['MODELPATH'])
        device = torch.device("cuda" if cfg['USE_CUDA'] and torch.cuda.is_available() else "cpu")
        print('yoohoo im here')
        # print(base_df)

        # Select one mini batch at one time. 
        # The mini batch is a list of tuples, where each tuple is a message and its timestamp.
    
        # Once we manage to set up the actual data, 
        # we will pull data from Kafka stream instead of having to retrieve 
        # data from local.
        data_df = pd.read_csv((cfg['EXPLOREPATH']+'/X.TESTINPUT'))

        # We will also be retrieving the age from the patient_id
        age = 60

        y_pred, y_prob = run_model(model, device, data_df, age)

        ## to see what "base_df" is like in the stream,
        ## Uncomment base_df.writeStream.outputMode(...)
        ## and comment out base_df.writeStream.foreachBatch(...)
        # query = base_df.writeStream.outputMode("append").format("console").trigger(processingTime='10 seconds').start()
        # query.awaitTermination()

        # Write the streaming data to MySQL using foreachBatch
        query = df.writeStream.foreachBatch(write_to_mysql).trigger(processingTime='10 seconds').start()
        # query.awaitTermination()

        for i in range(10):
            print("Model has successfully run!")
            print(y_pred)
            print(y_prob)
        
        # Store the results in y_pred and y_prob
        # run_model_dummy()

        # Create the first cursor for executing queries on the 'mytable' table
        cursor1 = cnx.cursor()
        query1 = 'SELECT * FROM mytable'
        cursor1.execute(query1)
        rows1 = cursor1.fetchall()
        print('Rows from mytable:')
        for idx, row in enumerate(rows1):
            # Too verbose
            if idx%50==0:
                print(row)
        
        query.awaitTermination()

        return query

    query = msg_process(consumer_conf['bootstrap.servers'], args.model_call_topic)

if __name__ == "__main__":
    main()