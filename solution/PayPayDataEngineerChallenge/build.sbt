name := "PayPayDataEngineerChallenge"

version := "0.1"

scalaVersion := "2.13.8"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

val sparkVersion = "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion %  Provided
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion % Provided

val jacksonVersion = "2.13.2"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "versions", _ @ _*)  => MergeStrategy.last
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
