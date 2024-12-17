import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ActivityLogApp {
    def main(args: Array[String]) : Unit = {
        // 추가 기간 처리를 위해 해당 로그 파일을 argument로 받음
        val n: Int = args.size
        var idx: Int = 0

        val spark = SparkSession.builder()
            .appName("SimpleApp")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .master("local[*]")
            .enableHiveSupport()
            .getOrCreate()

        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        import spark.implicits._


        if(spark.catalog.tableExists("activity_log") == false) {
            spark.sql("""CREATE EXTERNAL TABLE activity_log (
            event_time TIMESTAMP,
            event_type VARCHAR(20),
            product_id INT,
            category_id BIGINT,
            category_code VARCHAR(255),
            brand VARCHAR(255),
            price DOUBLE,
            user_id INT,
            user_session VARCHAR(50))
            PARTITIONED BY (partition_key DATE)
            STORED AS PARQUET
            LOCATION './hive_table/activity_log'""")
        }
        
        // 배치 장애시 복구를 위한 장치이며, 이미 처리된 데이터를 제외시키기 위한 원본테이블
        val original_df = spark.sql("""SELECT md5(concat_ws('|', event_time, event_type, product_id,category_id, category_code, brand,price,user_id, user_session)) as uuid FROM activity_log""")
        original_df.createOrReplaceTempView("original")

        val temp_df = spark.sql("DESCRIBE FORMATTED activity_log")
        val dir = temp_df.filter("col_name = 'Location'").select($"data_type").collect().toList(0)(0)
        val str_dir = dir.toString.replace("file:","")
        while (idx < n) {
            val inputFile = args(idx)
            println(inputFile)
            idx += 1

            val df = spark.read.option("header", "true").option("inferSchema", "true").csv(inputFile)

            val updatedDf = df.withColumn(
                "partition_key",
                to_date(col("event_time") + expr("INTERVAL 9 HOURS"))
            )
            val partitionedUpdatedDf = updatedDf.repartition(200)      
            partitionedUpdatedDf.createOrReplaceTempView("temp")
            
            // 배치 장애시 복구를 위한 장치이며, 이미 처리된 데이터를 제외
            val filteredDf = spark.sql("""SELECT
            t1.*
            FROM (
                SELECT *,md5(concat_ws('|', event_time, event_type, product_id,category_id, category_code, brand,price,user_id, user_session)) as uuid FROM temp) t1
            LEFT JOIN original t2
            ON t1.uuid = t2.uuid
            WHERE t2.uuid IS NULL""")
            if (filteredDf.count() != 0){
                println("************************************************************************************")
                val dropedDf = filteredDf.drop("uuid")
                dropedDf.write.partitionBy("partition_key").mode("append").option("compression","snappy").parquet(str_dir)
            }
        }
        spark.sql("MSCK REPAIR TABLE activity_log")
                        
        spark.stop()
    }
}