from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import time

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaSparkStreamingPeringatanGudang") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def safe_float_compare(value, threshold):
    if value is None:
        return False
    return value > threshold

def process_streams():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_bootstrap_servers = "localhost:29092"
        
        schema_suhu = StructType([
            StructField("gudang_id", StringType(), True),
            StructField("suhu", FloatType(), True),
            StructField("timestamp", FloatType(), True)
        ])

        schema_kelembaban = StructType([
            StructField("gudang_id", StringType(), True),
            StructField("kelembaban", FloatType(), True),
            StructField("timestamp", FloatType(), True)
        ])

        df_suhu = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", "sensor-suhu-gudang") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema_suhu).alias("data")) \
            .select(
                col("data.gudang_id").alias("gudang_id"),
                col("data.suhu"),
                from_unixtime(col("data.timestamp")).cast(TimestampType()).alias("timestamp")
            )

        df_kelembaban = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", "sensor-kelembaban-gudang") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), schema_kelembaban).alias("data")) \
            .select(
                col("data.gudang_id").alias("gudang_id"),
                col("data.kelembaban"),
                from_unixtime(col("data.timestamp")).cast(TimestampType()).alias("timestamp")
            )

        def process_suhu_batch(batch_df, batch_id):
            try:
                for row in batch_df.collect():
                    if safe_float_compare(row.suhu, 80):
                        print(f"[Peringatan Suhu Tinggi] Gudang {row.gudang_id}: Suhu {row.suhu}°C - {row.timestamp}")
            except Exception as e:
                print(f"Error processing temperature batch: {e}")

        def process_kelembaban_batch(batch_df, batch_id):
            try:
                for row in batch_df.collect():
                    if safe_float_compare(row.kelembaban, 70):
                        print(f"[Peringatan Kelembaban Tinggi] Gudang {row.gudang_id}: Kelembaban {row.kelembaban}% - {row.timestamp}")
            except Exception as e:
                print(f"Error processing humidity batch: {e}")

        query_suhu = df_suhu \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(process_suhu_batch) \
            .trigger(processingTime="2 seconds") \
            .option("checkpointLocation", "/tmp/checkpoint_suhu") \
            .start()

        query_kelembaban = df_kelembaban \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(process_kelembaban_batch) \
            .trigger(processingTime="2 seconds") \
            .option("checkpointLocation", "/tmp/checkpoint_kelembaban") \
            .start()

        df_suhu_wm = df_suhu.withWatermark("timestamp", "15 seconds")
        df_kelembaban_wm = df_kelembaban.withWatermark("timestamp", "15 seconds")

        join_condition = (df_suhu_wm["gudang_id"] == df_kelembaban_wm["gudang_id"]) & \
                         (df_suhu_wm["timestamp"] >= (df_kelembaban_wm["timestamp"] - expr("INTERVAL 10 SECONDS"))) & \
                         (df_suhu_wm["timestamp"] <= (df_kelembaban_wm["timestamp"] + expr("INTERVAL 10 SECONDS")))

        joined_df = df_suhu_wm.join(df_kelembaban_wm, join_condition, "inner") \
            .select(
                df_suhu_wm["gudang_id"],
                df_suhu_wm["suhu"],
                df_kelembaban_wm["kelembaban"],
                df_suhu_wm["timestamp"].alias("event_time")
            )

        def process_joined_batch(batch_df, batch_id):
            try:
                for row in batch_df.collect():
                    if row.suhu is None or row.kelembaban is None:
                        continue
                        
                    status = "Aman"
                    if safe_float_compare(row.suhu, 80) and safe_float_compare(row.kelembaban, 70):
                        status = "Bahaya tinggi! Barang berisiko rusak"
                    elif safe_float_compare(row.suhu, 80):
                        status = "Suhu tinggi, kelembaban normal"
                    elif safe_float_compare(row.kelembaban, 70):
                        status = "Kelembaban tinggi, suhu aman"
                    
                    print(f"[Status Gabungan] Gudang {row.gudang_id}:")
                    print(f"- Suhu: {row.suhu}°C")
                    print(f"- Kelembaban: {row.kelembaban}%")
                    print(f"- Status: {status}")
                    print("-" * 30)
            except Exception as e:
                print(f"Error processing joined batch: {e}")

        query_gabungan = joined_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(process_joined_batch) \
            .trigger(processingTime="2 seconds") \
            .option("checkpointLocation", "/tmp/checkpoint_gabungan") \
            .start()

        print("Streaming started. Waiting for data...")
        
        while True:
            time.sleep(5)
            if not all(q.isActive for q in [query_suhu, query_kelembaban, query_gabungan]):
                print("One or more queries have terminated unexpectedly")
                break

    except Exception as e:
        print(f"Error in streaming application: {e}")
    finally:
        print("Stopping all streaming queries...")
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    process_streams()