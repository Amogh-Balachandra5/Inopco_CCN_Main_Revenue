package utils

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Event Fact Load")
    .config("spark.master", "yarn")
    .config("spark.sql.warehouse.dir", "hdfs://172.23.87.139:8020/user/hive/warehouse")
    .config("spark.speculation", value = true)
    .config("spark.cores.max", "1")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.sql.tungsten.enabled", "true")
    .config("spark.debug.maxToStringFields",100)
    .enableHiveSupport()
    .getOrCreate()

}