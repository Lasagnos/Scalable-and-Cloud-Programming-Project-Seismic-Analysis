name := "EarthquakeAnalysis"
version := "1.0"

// Spark stabile su Scala 2.12
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  // 'provided' significa che la libreria è già presente sul cluster Google Cloud
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

// Permette di eseguire il codice in locale (tramite il tasto "Run" di IntelliJ)
Compile / run / fork := true

// Configurazione per creare il JAR finale (per sbt-assembly)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}