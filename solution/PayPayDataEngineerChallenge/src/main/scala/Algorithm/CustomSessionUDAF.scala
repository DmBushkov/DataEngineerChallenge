package Algorithm

import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{col, explode, udaf}

import scala.collection.mutable.ArrayBuffer

object CustomSessionUDAF {
  var idleTTLMicros: Long = 0
  case class CustomSessionBuffer(times: ArrayBuffer[Long])
  case class ResultSessionElement(timeMicros: Long, sessionID: Long)
  case class ResultSession(sessions: Array[ResultSessionElement])

  object AggSession extends Aggregator[Long, CustomSessionBuffer, ResultSession] {

    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: CustomSessionBuffer = CustomSessionBuffer(ArrayBuffer[Long]())

    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: CustomSessionBuffer, newTimestampMicros: Long): CustomSessionBuffer = {
      buffer.times += newTimestampMicros
      buffer
    }

    // Merge two intermediate values
    def merge(b1: CustomSessionBuffer, b2: CustomSessionBuffer): CustomSessionBuffer = {
      b1.times.appendAll(b2.times)
      b1
    }

    // Transform the output of the reduction
    def finish(reduction: CustomSessionBuffer): ResultSession = {
      val timesArray = reduction.times.toArray
      java.util.Arrays.sort(timesArray)
      var currSession: Long = timesArray(0)
      ResultSession(timesArray.indices.map { i => {
        if (i > 0 && timesArray(i) - timesArray(i - 1) > CustomSessionUDAF.idleTTLMicros) {
          currSession = timesArray(i)
        }
        ResultSessionElement(timesArray(i), currSession)
      }
      }.toArray)
    }

    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[CustomSessionBuffer] = Encoders.product

    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[ResultSession] = Encoders.product
  }

  def performAggregation(df: DataFrame, keyColumnsNames: Seq[String], timeAggColumn: String): DataFrame = {
    val sessionsUDAF = udaf(AggSession)
    val keyCols = keyColumnsNames.map(col)
    val resultCols = keyCols ++ Array(
      col("sessions.timeMicros").as(timeAggColumn), col("sessions.sessionID").as("session_id")
    )
    val sessionsDF = df
      .groupBy(keyCols: _*)
      .agg(sessionsUDAF(col(timeAggColumn)).as("agg_sessions"))
      .withColumn(colName="sessions", col = explode(col("agg_sessions.sessions")))
      .select(resultCols: _*)

    val leftDF = df.as("l")

    leftDF.join(
      sessionsDF,
      usingColumns = keyColumnsNames :+ timeAggColumn,
      joinType = "inner"
    ).select((df.columns.map(c => col("l." + c).as(c)) :+ col("session_id")): _*)

  }
}
