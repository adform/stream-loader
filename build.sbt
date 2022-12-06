name := "stream-loader"

ThisBuild / organization := "com.adform"
ThisBuild / organizationName := "Adform"
ThisBuild / scalaVersion := "2.13.10"
ThisBuild / scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-release", "8")

ThisBuild / startYear := Some(2020)
ThisBuild / licenses += ("MPL-2.0", new URL("http://mozilla.org/MPL/2.0/"))

ThisBuild / developers := List(
  Developer("sauliusvl", "Saulius Valatka", "saulius.vl@gmail.com", url("https://github.com/sauliusvl"))
)

enablePlugins(GitVersioning)
ThisBuild / git.useGitDescribe := true

ThisBuild / useCoursier := false

val gitRepo = "git@github.com:adform/stream-loader.git"
val gitRepoUrl = "https://github.com/adform/stream-loader"

val scalaTestVersion = "3.2.14"
val scalaCheckVersion = "1.17.0"
val scalaCheckTestVersion = "3.2.14.0"

lazy val `stream-loader-core` = project
  .in(file("stream-loader-core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    buildInfoPackage := s"${organization.value}.streamloader",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, git.gitHeadCommit),
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-reflect"     % scalaVersion.value,
      "org.apache.kafka"  % "kafka-clients"     % "3.3.1",
      "org.log4s"         %% "log4s"            % "1.10.0",
      "org.anarres.lzo"   % "lzo-commons"       % "1.0.6",
      "org.xerial.snappy" % "snappy-java"       % "1.1.8.4",
      "org.lz4"           % "lz4-java"          % "1.8.0",
      "com.github.luben"  % "zstd-jni"          % "1.5.2-5",
      "com.univocity"     % "univocity-parsers" % "2.9.1",
      "org.json4s"        %% "json4s-native"    % "4.0.6",
      "io.micrometer"     % "micrometer-core"   % "1.10.2",
      "org.scalatest"     %% "scalatest"        % scalaTestVersion % "test",
      "org.scalatestplus" %% "scalacheck-1-17"  % scalaCheckTestVersion % "test",
      "org.scalacheck"    %% "scalacheck"       % scalaCheckVersion % "test",
      "ch.qos.logback"    % "logback-classic"   % "1.4.5" % "test"
    ),
    testOptions += sbt.Tests.Setup(
      cl =>
        cl.loadClass("org.slf4j.LoggerFactory")
          .getMethod("getLogger", cl.loadClass("java.lang.String"))
          .invoke(null, "ROOT") // Prevents slf4j replay warnings during tests
    )
  )

lazy val `stream-loader-clickhouse` = project
  .in(file("stream-loader-clickhouse"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    resolvers += "jitpack" at "https://jitpack.io",
    libraryDependencies ++= Seq(
      "ru.yandex.clickhouse" % "clickhouse-jdbc"  % "0.3.1",
      "org.scalatest"        %% "scalatest"       % scalaTestVersion % "test",
      "org.scalatestplus"    %% "scalacheck-1-17" % scalaCheckTestVersion % "test",
      "org.scalacheck"       %% "scalacheck"      % scalaCheckVersion % "test"
    )
  )

val parquetVersion = "1.12.3"

lazy val `stream-loader-hadoop` = project
  .in(file("stream-loader-hadoop"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.sksamuel.avro4s" %% "avro4s-core"     % "4.1.0",
      "org.apache.parquet"  % "parquet-avro"     % parquetVersion,
      "org.apache.parquet"  % "parquet-protobuf" % parquetVersion,
      "org.apache.hadoop"   % "hadoop-client"    % "3.3.4" exclude ("log4j", "log4j"),
      "org.scalatest"       %% "scalatest"       % scalaTestVersion % "test"
    )
  )

lazy val `stream-loader-s3` = project
  .in(file("stream-loader-s3"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "s3"              % "2.17.192",
      "org.scalatest"          %% "scalatest"      % scalaTestVersion % "test",
      "com.amazonaws"          % "aws-java-sdk-s3" % "1.12.357" % "test",
      "org.gaul"               % "s3proxy"         % "2.0.0" % "test",
    )
  )

val verticaVersion = "9.2.1-0"
val verticaJarUrl = s"https://www.vertica.com/client_drivers/9.2.x/$verticaVersion/vertica-jdbc-$verticaVersion.jar"

lazy val `stream-loader-vertica` = project
  .in(file("stream-loader-vertica"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      (("com.vertica"     % "vertica-jdbc"     % verticaVersion) from verticaJarUrl) % "provided",
      "org.scalatest"     %% "scalatest"       % scalaTestVersion                    % "test",
      "org.scalatestplus" %% "scalacheck-1-17" % scalaCheckTestVersion               % "test",
      "org.scalacheck"    %% "scalacheck"      % scalaCheckVersion                   % "test"
    )
  )

lazy val packAndSplitJars =
  taskKey[(File, File)]("Runs pack and splits out the application jars from the external dependency jars")
lazy val dockerImage = settingKey[String]("Full docker image name")

lazy val `stream-loader-tests` = project
  .in(file("stream-loader-tests"))
  .dependsOn(`stream-loader-clickhouse`)
  .dependsOn(`stream-loader-hadoop`)
  .dependsOn(`stream-loader-s3`)
  .dependsOn(`stream-loader-vertica`)
  .configs(IntegrationTest)
  .enablePlugins(PackPlugin)
  .enablePlugins(DockerPlugin)
  .enablePlugins(BuildInfoPlugin)
  .settings(Defaults.itSettings: _*)
  .settings(headerSettings(IntegrationTest))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe"      % "config"           % "1.4.2",
      "ch.qos.logback"    % "logback-classic"  % "1.4.5",
      "com.zaxxer"        % "HikariCP"         % "5.0.1",
      "com.vertica"       % "vertica-jdbc"     % verticaVersion from verticaJarUrl,
      "org.scalacheck"    %% "scalacheck"      % scalaCheckVersion,
      "org.scalatest"     %% "scalatest"       % scalaTestVersion % "test,it",
      "org.scalatestplus" %% "scalacheck-1-17" % scalaCheckTestVersion % "test,it",
      ("com.spotify"      % "docker-client"    % "8.16.0" classifier "shaded") % "it"
    ),
    test := {}, // only integration tests present
    publish := {},
    publishLocal := {},
    publish / skip := true,
    buildInfoPackage := s"${organization.value}.streamloader",
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      version,
      scalaVersion,
      sbtVersion,
      git.gitHeadCommit,
      dockerImage
    ),
    packAndSplitJars := {
      val scalaMajorVersion = scalaVersion.value.split('.').take(2).mkString(".")
      val mainJar = s"${name.value}_$scalaMajorVersion-${version.value}.jar"
      val libDir = (Compile / pack).value / "lib"
      val appLibDir = (Compile / pack).value / "app-lib"
      appLibDir.mkdirs()
      IO.move(libDir / mainJar, appLibDir / mainJar)
      (libDir, appLibDir)
    },
    dockerImage := s"adform/${name.value}:${version.value}",
    docker / dockerfile := {

      val (depLib, appLib) = packAndSplitJars.value
      val lib = s"/opt/${name.value}/lib"
      val bin = s"/opt/${name.value}/bin/"

      new Dockerfile {
        from("eclipse-temurin:11.0.17_8-jre")

        env("APP_CLASS_PATH" -> s"$lib/*")

        runRaw(
          "apt-get update && " +
            "apt-get install -y --no-install-recommends zstd liblzo2-dev && " +
            "apt-get clean && rm -rf /var/lib/apt/lists/*")

        copy(depLib, lib) // add dependencies first to maximize docker cache usage
        copy(appLib, lib)

        add(file(name.value) / "bin" / "run-class.sh", bin)
        entryPoint(s"$bin/run-class.sh")

        label(
          "app.version" -> version.value,
          "app.git.hash" -> git.gitHeadCommit.value.get
        )
      }
    },
    docker / imageNames := {
      val Array(dockerImageName, dockerTag) = dockerImage.value.split(":")
      val Array(dockerNs, dockerRepo) = dockerImageName.split("/")
      Seq(
        ImageName(
          namespace = Some(dockerNs),
          repository = dockerRepo,
          tag = Some(dockerTag)
        )
      )
    },
    IntegrationTest / test := (IntegrationTest / test).dependsOn(docker).value,
    IntegrationTest / testOnly := (IntegrationTest / testOnly).dependsOn(docker).evaluated,
    // Prevents slf4j replay warnings during tests
    IntegrationTest / testOptions ++= Seq(
      sbt.Tests.Setup(
        cl =>
          cl.loadClass("org.slf4j.LoggerFactory")
            .getMethod("getLogger", cl.loadClass("java.lang.String"))
            .invoke(null, "ROOT")
      ),
      Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3", "-minSuccessfulTests", "10")
    ),
    inConfig(IntegrationTest)(org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings)
  )

lazy val generateDiagrams = taskKey[Seq[File]]("Renders UML diagrams to SVG images")

lazy val commonSettings = Seq(
  generateDiagrams := {
    import net.sourceforge.plantuml._

    val sourceDir = baseDirectory.value / "src" / "main" / "diagrams"
    val targetDir = target.value / "diagrams"
    targetDir.mkdirs()

    IO.listFiles(sourceDir)
      .filter(_.getPath.endsWith(".puml"))
      .map(file => {
        val uml = new SourceStringReader(IO.read(file))
        val os = new java.io.ByteArrayOutputStream(8192)
        val outFile = targetDir / (file.getName.substring(0, file.getName.lastIndexOf('.')) + ".svg")

        val outDesc = uml.outputImage(os, new FileFormatOption(FileFormat.SVG))
        if (outDesc.getDescription.toLowerCase.contains("error"))
          throw new IllegalStateException(s"Error rendering the UML diagram for ${file}")
        IO.write(outFile, os.toByteArray)
        outFile
      })
  },
  publishMavenStyle := true,
  Test / publishArtifact := false,
  publishTo := sonatypePublishToBundle.value,
  homepage := Some(url(gitRepoUrl)),
  scmInfo := Some(ScmInfo(url(gitRepoUrl), s"scm:git:$gitRepo"))
)

lazy val copyDocAssets = taskKey[File]("Copy unidoc resources")

import com.github.sbt.git.SbtGit.GitKeys._

lazy val `stream-loader` = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .enablePlugins(GhpagesPlugin)
  .settings(
    publish := {},
    publishLocal := {},
    publish / skip := true,
    Compile / doc / scalacOptions ++= Seq("-doc-title", name.value),
    ScalaUnidoc / unidoc / unidocProjectFilter := inAnyProject -- inProjects(`stream-loader-tests`),
    copyDocAssets := Def.taskDyn {
      val filter = ScopeFilter(inProjects(thisProject.value.aggregate: _*))
      val destination = thisProject.value.base / "target" / "diagrams"
      destination.mkdirs()
      Def.task {
        generateDiagrams
          .all(filter)
          .value
          .flatten
          .foreach(diagram => IO.copyFile(diagram, destination / diagram.getName))
        destination
      }
    }.value,
    makeSite := makeSite.dependsOn(Compile / unidoc).value,
    addMappingsToSiteDir(ScalaUnidoc / packageDoc / mappings, ScalaUnidoc / siteSubdirName),
    ScalaUnidoc / siteSubdirName := "",
    makeSite / mappings ++= {
      val diagramDir = copyDocAssets.value
      diagramDir
        .listFiles()
        .toSeq
        .map(diagram => diagram -> ("diagrams/" + diagramDir.toURI.relativize(diagram.toURI).getPath))
    },
    gitRemoteRepo := s"$gitRepoUrl.git",
    ghpagesRepository := file(s"/tmp/ghpages/${organization.value}/${name.value}/${version.value}")
  )
  .aggregate(
    `stream-loader-core`,
    `stream-loader-clickhouse`,
    `stream-loader-hadoop`,
    `stream-loader-s3`,
    `stream-loader-vertica`,
    `stream-loader-tests`
  )
