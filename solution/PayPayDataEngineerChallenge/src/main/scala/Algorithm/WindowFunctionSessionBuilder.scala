package Algorithm

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WindowFunctionSessionBuilder {
  var idleTTLMicros: Long = 0

  def performAggregation(df: DataFrame, keyColumnsNames: Seq[String], timeAggColumn: String): DataFrame = {
    val keyCols = keyColumnsNames.map(col)
    val aggCol = col(timeAggColumn)
    val getPrevTimeWindow = Window.partitionBy(keyCols: _*).orderBy(aggCol)

    df
      .withColumn("prev_event_time", lag(aggCol, 1).over(getPrevTimeWindow))
      .withColumn("session_id",
        when(
          (col("prev_event_time").isNull) or (
            aggCol.minus(col("prev_event_time")).gt(lit(idleTTLMicros))
            ),
          aggCol
        ).otherwise(lit(null))
      )
      .withColumn("session_id", max("session_id").over(getPrevTimeWindow))
      .select((df.columns :+ "session_id").map(col): _*)
  }
}


