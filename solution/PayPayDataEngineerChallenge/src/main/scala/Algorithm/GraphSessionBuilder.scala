package Algorithm

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.graphx.lib.ConnectedComponents
import org.apache.spark.graphx._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._



object GraphSessionBuilder {
  var idleTTLMicros: Long = 0


  def performGraphClustering(df: DataFrame,
                             sparkSession: SparkSession,
                             keyColumnsNames: Seq[String],
                             timeAggColumn: String,
                             iters: Int = 100): DataFrame = {
    val enumerateRowsWindow = Window.partitionBy().orderBy((keyColumnsNames :+ timeAggColumn).map(col): _*)
    val getNextHitWindow = Window.partitionBy(keyColumnsNames.map(col): _*).orderBy(timeAggColumn)

    val enumeratedRows = df
      .withColumn("rn", row_number().over(enumerateRowsWindow))
      .withColumn("next_rn", lead(col("rn"), 1).over(getNextHitWindow))
      .withColumn(
        "next_event_time_micros",
        lead(col(timeAggColumn), 1).over(getNextHitWindow)
      ).cache()

    val edges = enumeratedRows
      .where(
        (col("next_rn").isNotNull) and (
        (col("next_event_time_micros") - col(timeAggColumn)).leq(lit(idleTTLMicros))
        )
      )
      .select("rn", "next_rn").as[(Long, Long)](Encoders.product)
      .rdd.map{case (l, r) => Edge[Long](l, r)}
    val graph = Graph.fromEdges(edges = edges, defaultValue = 0)

    val connectedComponentsVertices = ConnectedComponents.run(graph, iters).vertices

    val componentsSchema = StructType(Seq(StructField("init_id", LongType), StructField("session_id", LongType)))
    val idsDF = sparkSession.createDataFrame(
      connectedComponentsVertices.map{case (v1, v2) => Row.fromSeq(Seq(v1, v2))},
      componentsSchema
    )

    enumeratedRows.join(
      idsDF,
      col("rn") === col("init_id"),
      joinType = "inner"
    ).select((df.columns :+ "session_id").map(col): _*)

  }





}
