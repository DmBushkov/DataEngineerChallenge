package Utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scopt.{OParser, OParserBuilder}

import java.nio.file.{Files, Paths}
import java.io.{BufferedReader, FileReader}


// Argument parser for app
object ArgParser {
  // configuration case class
  case class CommandLineConfig(inputHitsData: String = "",
                               hitsIdleTimeMin: Int = 30,
                               inputDataSchema: Option[InputSchema] = None,
                               sessionBuildingMethod: String = eWindowsMethod,
                               graphMetodIterations: Int = 100,
                               outputDataPath: Option[String] = None)

  // helpful classes: required to parse input schema from YAML
  case class InputSchemaElement(name: String, `type`: String)
  case class InputSchema(schema: List[InputSchemaElement])

  val eCustomAggregationMethod: String = "custom_agg"
  val eGraphMethod: String = "graph"
  val eWindowsMethod: String = "window"
  private val allowedSessionBuildingMethods: Set[String] = Set(
    eCustomAggregationMethod,
    eGraphMethod,
    eWindowsMethod
  )


  // command line args parsing itself
  def parseArgs(args: Array[String]): CommandLineConfig = {
    val builder: OParserBuilder[CommandLineConfig] = OParser.builder[CommandLineConfig]
    val cliParser = {
      OParser.sequence(
        builder.programName(
          "Solution for PayPay's Data Engineer Challenge: https://github.com/Pay-Baymax/DataEngineerChallenge"
        ),
        builder.opt[String]('i', "input_hits")  // option --input_hits/-i
          .action((strVal, conf) => conf.copy(inputHitsData = strVal))
          .required()
          .text("Input path to hits log. Use `file:///path/to/data` if file is local."),
        builder.opt[String]('s', "input_schema")  // option --input_schema/-s
          .validate(path =>
            if (Files.exists(Paths.get(path)) && path.endsWith("yaml")) builder.success
            else builder.failure(s"Option -s/--input_schema should refer to existing file but `$path` got.")
          )
          .validate(path =>
            if (path.endsWith("yaml") || path.endsWith("yml")) builder.success
            else builder.failure(s"Option -s/--input_schema should refer to YAML-file, but `$path` got")
          )
          .action((strVal, conf) => conf.copy(inputDataSchema = Option(parseSchema(strVal))))
          .required()
          .text("Path to a schema for input hits data. Should be an existing valid local YAML-file."),
        builder.opt[Int]("idle_time")  // option --idle_time, optional, by default 30 min.
          .action((intVal, conf) => conf.copy(hitsIdleTimeMin = intVal))
          .optional()
          .validate(ttl =>
            if (ttl > 0) builder.success
            else builder.failure(s"Option --idle_time should be positive, but `$ttl` got.")
          )
          .text("Time period that separates two adjacent sessions."),
        builder.opt[String]('m', "method")  // option -m/--method
          .action((strVal, conf) => conf.copy(sessionBuildingMethod = strVal))
          .optional()
          .validate(method =>
            if (allowedSessionBuildingMethods(method)) builder.success
            else builder.failure(
              "Option -m/--method should contain one of " +
                s"allowed `${allowedSessionBuildingMethods.mkString(", ")}` methods, but `$method` got."
            )
          )
          .text(
            "Method for a session building algorithm: custom aggregating function or graph connected components."
          ),
        builder.opt[String]('o', "output_result")  // option --output_result/-o
          .action((strVal, conf) => conf.copy(outputDataPath = Some(strVal)))
          .optional()
          .text("Output path for processing result. Use `file:///path/to/data` if file is local."),
        builder.opt[Int]("graph_iters")
          .action((intVal, conf) => conf.copy(graphMetodIterations = intVal))
          .optional()
          .text("Iterations quantity for graph method.")
      )
    }
    OParser.parse(cliParser, args, CommandLineConfig()) match {
      case Some(config) => config
      case _ =>
        System.err.println("Failed to parse command line arguments!")
        sys.exit(-1)
    }
  }

  // Function to parse schema for input DF from local YAML
  private def parseSchema(schemaPath: String): InputSchema = {
    val fileReader: FileReader = new FileReader(schemaPath)
    val bufferedReader: BufferedReader = new BufferedReader(fileReader)

    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)

    val resultConfig = mapper.readValue(bufferedReader, classOf[InputSchema])

    bufferedReader.close()
    fileReader.close()

    resultConfig
  }
}
