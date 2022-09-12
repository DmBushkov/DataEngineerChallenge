package Main

import Utils.ArgParser
import Utils.SparkIO._
import Algorithm.DataCleanUp.cleanBadHits
import Algorithm.{CustomSessionUDAF, GraphSessionBuilder, WindowFunctionSessionBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {
    val cliConfig:ArgParser.CommandLineConfig = ArgParser.parseArgs(args)
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("PayPay Data Engineer Challenge")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val inputDF = readDataToDF(sparkSession, cliConfig)

    val transformedDF = inputDF
      .withColumn("client_ip_port_splitted", split(trim(col("client_ip_port")), ":"))
      .withColumns(
        Map(
          "client_ip" -> col("client_ip_port_splitted")(lit(0)),
          "client_port" -> col("client_ip_port_splitted")(lit(1))
        )
      )
      .withColumn("splitted_http", split(trim(col("request"), "\""), " "))
      .withColumn("parts_len", size(col("splitted_http")))  // get length, according to data: always 2 or 3
      .select(
        col("event_time_utc"),
        (col("event_time_utc").cast(DoubleType) * lit(1000000)).cast(LongType).as("event_time_micros"),  // // let's get microseconds from timestamp, helpful to count diff between hits
        col("client_ip"),
        col("client_port"),
        trim(trim(trim(col("splitted_http").getItem(0)), "\""), "\t").as("method"),  // method is always first
        col("splitted_http")(lit(1)).as("url"),  // url is always second
        trim(
          trim(
            when(col("parts_len") === 2,  // sometimes http-version is omitted or  moved to another field
              when(col("user_agent").startsWith("HTTP"), col("user_agent")).otherwise("")  // Few lines has shifted columns
            ).otherwise(
              col("splitted_http")(lit(2))
            ),
            "\""
          ),
          "\t"
        ).as("http_version"),
        when(col("user_agent").startsWith("HTTP"), col("ssl_cipher"))
          .otherwise(col("user_agent")).as("user_agent")
      )

    val cleanDF = cleanBadHits(transformedDF)

    val delayBetweenSessionsMicros = cliConfig.hitsIdleTimeMin * 60 * 1000000
    val aggregationKeyColumns = Seq("client_ip", "user_agent")
    val aggregationTimeColumn = "event_time_micros"

    val sessionedDF = cliConfig.sessionBuildingMethod match {
      case ArgParser.eCustomAggregationMethod =>
        CustomSessionUDAF.idleTTLMicros = delayBetweenSessionsMicros
        CustomSessionUDAF.performAggregation(cleanDF, aggregationKeyColumns, aggregationTimeColumn)
      case ArgParser.eGraphMethod =>
        GraphSessionBuilder.idleTTLMicros = delayBetweenSessionsMicros
        GraphSessionBuilder.performGraphClustering(
          cleanDF,
          sparkSession,
          aggregationKeyColumns,
          aggregationTimeColumn,
          cliConfig.graphMetodIterations
        )
      case ArgParser.eWindowsMethod =>
        WindowFunctionSessionBuilder.idleTTLMicros = delayBetweenSessionsMicros
        WindowFunctionSessionBuilder.performAggregation(cleanDF, aggregationKeyColumns, aggregationTimeColumn)
    }

    val sessionedDFCached = sessionedDF.cache()

//    Determine the average session time
//    Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
//    Find the most engaged users, ie the IPs with the longest session times
    val forAvgSessionTime = sessionedDFCached
      .groupBy((aggregationKeyColumns :+ "session_id").map(col): _*)  // session ids may be not unique globally, only for every (ip, user_agent)-pair
      .agg(
        min(col(aggregationTimeColumn)).as("first_hit_micros"),
        max(col(aggregationTimeColumn)).as("last_hit_micros"),
        countDistinct(col("url")).as("urls_visited"),
        sum(lit(1)).as("hits_cnt")
      )
      .withColumn("session_duration_sec", (col("last_hit_micros") - col("first_hit_micros")).divide(lit(1000000)))
      .orderBy(col("session_duration_sec").desc)

    val averageMetrics = forAvgSessionTime
      .agg(
        avg(col("urls_visited")).as("avg_urls_visited"),
        avg(col("session_duration_sec")).as("avg_session_duration_sec")
      )

    if (cliConfig.outputDataPath.isDefined) {
      writeDF(sessionedDFCached, cliConfig.outputDataPath.get + "/aggregated_sessions", compress = true)
      writeDF(averageMetrics, cliConfig.outputDataPath.get + "/average_metrics")
      writeDF(forAvgSessionTime, cliConfig.outputDataPath.get + "/from_longest_session_to_shortest")
    } else {
      sessionedDFCached.show(50)
      averageMetrics.show(100, truncate = false)
      forAvgSessionTime.show(50)
    }
  }
}
