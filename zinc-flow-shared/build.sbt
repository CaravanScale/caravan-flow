ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / javacOptions ++= Seq("--release", "25")

lazy val root = (project in file("."))
  .settings(
    name := "zinc-flow-shared",
    crossPaths := false,
    autoScalaLibrary := false,
    libraryDependencies ++= Seq(
      // Jackson annotations only — consumers pull in jackson-databind
      // themselves. The DTOs are pure records + @JsonProperty hints so
      // the wire format stays stable across casing differences.
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.0",
      "com.github.sbt.junit" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.junit.jupiter" % "junit-jupiter" % "6.0.3" % Test,
      "org.junit.platform" % "junit-platform-launcher" % "6.0.3" % Test,
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.0" % Test
    ),
    testOptions += Tests.Argument(jupiterTestFramework, "--display-mode=tree")
  )
