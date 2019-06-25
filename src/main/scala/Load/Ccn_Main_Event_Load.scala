package Load

import utils.{Context, Dim_Data}
import Driver.fact_event_load
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.LongType


class Ccn_Main_Event_Load extends Context {

  def CCN_Main_load(): Unit = {

    val ccn_main_data = sparkSession.sql("select NETWORK_PLATFORM_FLAG,CALL_TYPE,SERVICE_CLASS,CALL_LEG_TYPE,CALL_START_TIME,CALL_START_DATE,IMEI,MAIN_CHARGED_AMOUNT,PRIMARY_DEDUCTION_ACCOUNT_ID,BAL_BALANCE_TYPE,BUFFER1,DEBIT_AMOUNT,DEBIT_FROM_PREPAID,CHARGED_DURATION,DATA_VOLUME,UPLOAD_DATA_VOLUME,DOWNLOAD_DATA_VOLUME,source_name,ACCESS_METHOD_IDENTIFIER,LAC_ID,CELL_ID from staging.ccn_main")

    val ccn_main_lookup_df = ccn_main_data

      .withColumn("NETWROK_TECH_LOOKUP_KEY", lit(when(col("NETWORK_PLATFORM_FLAG").equalTo(1), "A0100").otherwise(when(col("NETWORK_PLATFORM_FLAG").equalTo(2), "A0200").otherwise(when(col("NETWORK_PLATFORM_FLAG").equalTo(6), "A0300").otherwise("UNKNOWN")))))
      .join(broadcast(Dim_Data.dim_network_tech), col("NETWROK_TECH_LOOKUP_KEY")
        === Dim_Data.dim_network_tech.col("unq_cd_src_stm"), "Left")
      .drop("unq_cd_src_stm")

      .withColumn("SERVICE_USAGE_ACTIVITY_LOOKUP_KEY", lit(when(col("CALL_TYPE").isNull, "UNKNOWN").otherwise(col("CALL_TYPE"))))
      .join(Dim_Data.dim_service_usage_activity, col("SERVICE_USAGE_ACTIVITY_LOOKUP_KEY")
        === Dim_Data.dim_service_usage_activity.col("unq_cd_src_stm"), "Left")
      .drop("unq_cd_src_stm")

      .withColumn("SERVICE_CLASS_LOOKUP_KEY", lit(when(col("SERVICE_CLASS").isNull, "UNKNOWN").otherwise(lit(upper(col("SERVICE_CLASS"))))))
      .join(Dim_Data.dim_service_class, col("SERVICE_CLASS_LOOKUP_KEY")
        === Dim_Data.dim_service_class.col("UNQ_CD_SRC_STM"), "Left")
      .drop("UNQ_CD_SRC_STM")

      .withColumn("SERVICE_TYPE_LOOKUP_KEY", lit(when(col("CALL_LEG_TYPE").isNull, "UNKNOWN").otherwise(lit(upper(col("CALL_LEG_TYPE"))))))
      .join(Dim_Data.dim_service_type, col("SERVICE_TYPE_LOOKUP_KEY")
        === Dim_Data.dim_service_type.col("UNQ_CD_SRC_STM"), "Left")
      .drop("UNQ_CD_SRC_STM")

      .withColumn("HOUR_LOOKUP_KEY", lit(substring(col("CALL_START_TIME"), 1, 2)))
      //Tested
      .withColumn("DATE_LOOKUP_KEY", lit(concat_ws("-", substring(col("CALL_START_DATE"), 1, 4), substring(col("CALL_START_DATE"), 5, 2), substring(col("CALL_START_DATE"), 7, 2))))
      .join(Dim_Data.dim_date, col("DATE_LOOKUP_KEY")
        === Dim_Data.dim_date.col("fct_date"), "Left")
      .drop("fct_date")

      .withColumn("DEVICE_LOOKUP_KEY", lit(when(substring(col("IMEI"), 1, 8).isNull, "UNKNOWN").otherwise(substring(col("IMEI"), 1, 8))))
      .join(Dim_Data.dim_access_device, col("DEVICE_LOOKUP_KEY")
        === Dim_Data.dim_access_device.col("tac_code"), "Left")
      .drop("tac_code")

      .withColumn("SOURCE_OBJ_LOOKUP_KEY", lit(upper(col("source_name"))))
      .join(broadcast(Dim_Data.dim_source_object), col("SOURCE_OBJ_LOOKUP_KEY")
        === Dim_Data.dim_source_object.col("unq_cd_src_stm"), "Left")
      .drop("unq_cd_src_stm")

      .withColumn("GEO_LOOKUP_KEY", lit(concat_ws("-", substring(col("LAC_ID"), 2, 5), col("CELL_ID"))))
      .join(Dim_Data.dim_geo_master, col("GEO_LOOKUP_KEY")
        === Dim_Data.dim_geo_master.col("cell_code"), "Left")
      .drop("cell_code")

      .where(concat_ws("-", substring(col("CALL_START_DATE"), 1, 4), substring(col("CALL_START_DATE"), 5, 2), substring(col("CALL_START_DATE"), 7, 2)).equalTo(fact_event_load.date_enter))

      .select("geo_key", "ACCESS_METHOD_IDENTIFIER", "DATE_LOOKUP_KEY", "HOUR_LOOKUP_KEY", "date_key"
        , "tac_key", "source_object_key", "network_tech_key", "service_usage_activity_key", "service_class_key"
        , "SERVICE_USAGE_ACTIVITY_LOOKUP_KEY", "CHARGED_DURATION", "CALL_TYPE", "DATA_VOLUME"
        , "UPLOAD_DATA_VOLUME", "DOWNLOAD_DATA_VOLUME", "BAL_BALANCE_TYPE", "PRIMARY_DEDUCTION_ACCOUNT_ID",
        "MAIN_CHARGED_AMOUNT", "BUFFER1", "DEBIT_FROM_PREPAID", "DEBIT_AMOUNT", "CALL_START_DATE", "SERVICE_CLASS"
        , "SERVICE_TYPE_LOOKUP_KEY", "service_type_key")

    ccn_main_lookup_df.show(15)

    //val ccn_grouped : DataFrame = ccn_main_lookup_df
    //  .groupBy(col("date_key"),col("service_usage_activity_key"),col
    //("service_class_key"),col("SERVICE_USAGE_ACTIVITY_LOOKUP_KEY"),col("service_type_key")
    //  ,col("network_tech_key"),col("tac_key"),col("source_object_key")
    //  ,col("HOUR_LOOKUP_KEY"),col("DATE_LOOKUP_KEY")
    //  ,col("CALL_TYPE")
    //  ,col("SERVICE_CLASS")
    //  ,col("SERVICE_TYPE_LOOKUP_KEY")
    //  ,col("ACCESS_METHOD_IDENTIFIER")
    //  ,col("geo_key"))
    //
    //  .agg(count(lit("1")) as "EVENT_COUNT"
    //
    //    ,sum(when(upper(col("CALL_TYPE")).equalTo("VOICE") , when(col("CHARGED_DURATION").equalTo(-99) || col("CHARGED_DURATION").isNull , 0).otherwise(col("CHARGED_DURATION").cast(LongType))).otherwise(0))/60 as "MIN_OF_USAGE"
    //
    //    ,sum(when(col("CALL_TYPE").equalTo("DATA"),col("DATA_VOLUME").cast(LongType)/1024/1024).otherwise(0))as "TOTAL_DATA_VOLUMN"
    //
    //    ,sum(when(col("CALL_TYPE").equalTo("DATA"),col("UPLOAD_DATA_VOLUME").cast(LongType)/1024/1024).otherwise(0)) as "TOTAL_UPLOAD_VOLUMN"
    //
    //    ,sum(when(col("CALL_TYPE").equalTo("DATA"),col("DOWNLOAD_DATA_VOLUME").cast(LongType)/1024/1024).otherwise(0)) as "TOTAL_DOWNLOAD_VOLUMN"
    //
    //    ,sum(when(upper(col("CALL_TYPE")).isin("MOC","RCF","CF","ISDNO","ISDNCF","MTC","MT","MO"),when(col("BAL_BALANCE_TYPE").equalTo(2000)|| col("PRIMARY_DEDUCTION_ACCOUNT_ID").equalTo(2505),col("MAIN_CHARGED_AMOUNT").cast(LongType)+ col("BUFFER1").cast(LongType)).otherwise(0)).otherwise(when(col("CALL_TYPE").equalTo("DATA"),when(col("DEBIT_AMOUNT") > 0, col("DEBIT_FROM_PREPAID")).otherwise(0)).otherwise(0))) as "BILLD_AMT")
    //
    //  .select("network_tech_key"
    //    ,"service_usage_activity_key"
    //    ,"service_class_key"
    //    ,"SERVICE_USAGE_ACTIVITY_LOOKUP_KEY"
    //    ,"service_type_key"
    //    ,"HOUR_LOOKUP_KEY"
    //    ,"date_key"
    //    ,"tac_key"
    //    ,"BILLD_AMT"
    //    ,"EVENT_COUNT"
    //    ,"EVENT_COUNT"
    //    ,"MIN_OF_USAGE"
    //    ,"total_data_volumn"
    //    ,"TOTAL_UPLOAD_VOLUMN"
    //    ,"TOTAL_DOWNLOAD_VOLUMN"
    //    ,"source_object_key"
    //    ,"DATE_LOOKUP_KEY"
    //    ,"ACCESS_METHOD_IDENTIFIER"
    //    ,"ACCESS_METHOD_IDENTIFIER"
    //    ,"geo_key")
    //
    //    ccn_grouped.show(5)
    //    ccn_grouped.write.mode("append").insertInto("analytics.fct_events")
  }
}
