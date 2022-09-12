package Utils

import Utils.ArgParser.{CommandLineConfig, InputSchema, InputSchemaElement}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object SparkIO {
  // transform Utils.ArgParser.InputSchema into Spark StructType
  private def transformSchema(schema: InputSchema): StructType = {
    StructType(
      schema.schema
        .map{case InputSchemaElement(nm, tp) =>
          StructField(
            nm,
            tp match {
              case "IntegerType" => IntegerType
              case "LongType" => LongType
              case "StringType" => StringType
              case "DoubleType" => DoubleType
              case "TimestampType" => TimestampType
            }
          )
        }
    )
  }

  // Reads data according to configuration object
  // Options and format could be also passed via CLI-arguments and configuration files
  def readDataToDF(sparkSession: SparkSession, config: CommandLineConfig): DataFrame = {
    sparkSession
      .read
      .format("csv")
      .option("sep", " ")
      .option("header", "false")
      .option("quote", "\"")
      .schema(transformSchema(config.inputDataSchema.get))
      .load(config.inputHitsData)
  }

  def writeDF(df: DataFrame, outputPath: String, compress: Boolean = false): Unit = {
    val dfWriter = df
      .repartition(1)
      .write
      .format("csv")
      .option("sep", " ")
      .option("header", "true")
      .option("quote", "\"")

    if (compress) dfWriter.option("compression", "gzip")

    dfWriter.save(outputPath)
  }
}
