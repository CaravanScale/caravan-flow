ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / javacOptions ++= Seq("--release", "25")

// Reference the sibling shared module directly from disk — no
// publish step required. sbt compiles shared as part of this build.
lazy val shared = RootProject(file("../zinc-flow-shared"))

lazy val root = (project in file("."))
  .dependsOn(shared)
  .settings(
    name := "zinc-flow-ui-java",
    crossPaths := false,
    autoScalaLibrary := false,
    Compile / mainClass := Some("zincflow.ui.UiMain"),
    assembly / mainClass := Some("zincflow.ui.UiMain"),
    assembly / assemblyJarName := "zinc-flow-ui.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case other =>
        val defaultStrategy = (assembly / assemblyMergeStrategy).value
        defaultStrategy(other)
    },
    libraryDependencies ++= Seq(
      "io.javalin"                   % "javalin"              % "6.3.0",
      "io.pebbletemplates"           % "pebble"               % "3.2.2",
      "com.fasterxml.jackson.core"   % "jackson-databind"     % "2.18.0",
      "org.slf4j"                    % "slf4j-api"            % "2.0.16",
      "ch.qos.logback"               % "logback-classic"      % "1.5.12",
      "com.github.sbt.junit"         % "jupiter-interface"    % JupiterKeys.jupiterVersion.value % Test,
      "org.junit.jupiter"            % "junit-jupiter"        % "6.0.3" % Test,
      "org.junit.platform"           % "junit-platform-launcher" % "6.0.3" % Test
    ),
    testOptions += Tests.Argument(jupiterTestFramework, "--display-mode=tree")
  )
