enablePlugins(JavaAppPackaging)

organization := "org.geneontology"

name := "golr-loader"

version := "0.2"

scalaVersion := "2.13.11"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

mainClass in Compile := Some("org.geneontology.golr.Main")

javaOptions += "-Xmx10G"

lazy val zioVersion = "2.0.15"
lazy val sttpVersion = "3.8.16"

libraryDependencies ++= {
  Seq(
    "org.apache.jena"               %  "apache-jena-libs" % "4.5.0", // newer Jena versions have problems with too many file locks
    "com.softwaremill.sttp.client3" %% "zio"              % sttpVersion,
    "com.softwaremill.sttp.client3" %% "zio-json"         % sttpVersion,
    "dev.zio"                       %% "zio"              % zioVersion,
    "dev.zio"                       %% "zio-streams"      % zioVersion,
    "dev.zio"                       %% "zio-json"         % "0.6.0",
    "org.phenoscape"                %% "sparql-utils"     % "1.3.1",
    "com.outr"                      %% "scribe-slf4j"     % "3.11.9"
  )
}
