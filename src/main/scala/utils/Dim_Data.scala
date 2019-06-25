package utils

import org.apache.spark.sql.DataFrame

object Dim_Data extends Context {

    val dim_network_tech : DataFrame  = sparkSession.sql ("select network_tech_key, unq_cd_src_stm from analytics.dim_network_tech")

    val dim_service_usage_activity : DataFrame = sparkSession.sql ("select service_usage_activity_key,unq_cd_src_stm from analytics.dim_service_usage_activity")

    val dim_service_class : DataFrame = sparkSession.sql ("select service_class_key,unq_cd_src_stm from analytics.dim_service_class")

    val dim_service_type : DataFrame = sparkSession.sql ("select service_type_key, unq_cd_src_stm from analytics.dim_service_type")

    val dim_date : DataFrame = sparkSession.sql ("select fct_date,date_key from analytics.dim_date")

    val dim_access_device : DataFrame = sparkSession.sql ("select tac_key,tac_code from analytics.dim_access_device_master")

    val dim_source_object : DataFrame = sparkSession.sql ("select unq_cd_src_stm,source_object_key from analytics.dim_source_object")

    val dim_geo_master : DataFrame = sparkSession.sql ("select cell_code,geo_key from analytics.dim_geo_master")
  }