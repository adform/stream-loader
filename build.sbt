name := "stream-loader"

ThisBuild / organization := "com.adform"
ThisBuild / organizationName := "Adform"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / scalacOptions := Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-encoding",
  "utf8",
  "-release",
  "11",
  "-Wconf:msg=While parsing annotations in:silent"
)

ThisBuild / startYear := Some(2020)
ThisBuild / licenses += ("MPL-2.0", url("http://mozilla.org/MPL/2.0/"))

ThisBuild / developers := List(
  Developer("sauliusvl", "Saulius Valatka", "saulius.vl@gmail.com", url("https://github.com/sauliusvl"))
)

enablePlugins(GitVersioning)

val gitRepo = "adform/stream-loader"
val gitRepoUrl = s"https://github.com/$gitRepo"

ThisBuild / git.useGitDescribe := true
ThisBuild / git.remoteRepo := {
  sys.env.get("GITHUB_TOKEN") match {
    case Some(token) => s"https://x-access-token:$token@github.com/$gitRepo"
    case None => s"git@github.com:$gitRepo.git"
  }
}

val scalaTestVersion = "3.2.19"
val scalaCheckVersion = "1.18.0"
val scalaCheckTestVersion = "3.2.19.0"

val hadoopVersion = "3.4.0"
val parquetVersion = "1.14.1"
val icebergVersion = "1.6.0"

lazy val `stream-loader-core` = project
  .in(file("stream-loader-core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    buildInfoPackage := s"${organization.value}.streamloader",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, git.gitHeadCommit),
    libraryDependencies ++= Seq(
      "org.scala-lang"     % "scala-reflect"     % scalaVersion.value,
      "org.apache.kafka"   % "kafka-clients"     % "3.8.0",
      "org.log4s"         %% "log4s"             % "1.10.0",
      "org.apache.commons" % "commons-compress"  % "1.26.2",
      "org.xerial.snappy"  % "snappy-java"       % "1.1.10.5",
      "org.lz4"            % "lz4-java"          % "1.8.0",
      "com.github.luben"   % "zstd-jni"          % "1.5.6-4",
      "com.univocity"      % "univocity-parsers" % "2.9.1",
      "org.json4s"        %% "json4s-native"     % "4.0.7",
      "io.micrometer"      % "micrometer-core"   % "1.13.2",
      "org.scalatest"     %% "scalatest"         % scalaTestVersion      % "test",
      "org.scalatestplus" %% "scalacheck-1-18"   % scalaCheckTestVersion % "test",
      "org.scalacheck"    %% "scalacheck"        % scalaCheckVersion     % "test",
      "ch.qos.logback"     % "logback-classic"   % "1.5.6"               % "test"
    )
  )

lazy val `stream-loader-clickhouse` = project
  .in(file("stream-loader-clickhouse"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    resolvers += "jitpack" at "https://jitpack.io",
    libraryDependencies ++= Seq(
      "org.apache.httpcomponents.client5" % "httpclient5"     % "5.3.1",
      "com.clickhouse"                    % "clickhouse-jdbc" % "0.6.3",
      "org.scalatest"                    %% "scalatest"       % scalaTestVersion      % "test",
      "org.scalatestplus"                %% "scalacheck-1-18" % scalaCheckTestVersion % "test",
      "org.scalacheck"                   %% "scalacheck"      % scalaCheckVersion     % "test"
    )
  )

lazy val `stream-loader-hadoop` = project
  .in(file("stream-loader-hadoop"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.sksamuel.avro4s" %% "avro4s-core"      % "4.1.2",
      "org.apache.parquet"   % "parquet-avro"     % parquetVersion,
      "org.apache.parquet"   % "parquet-protobuf" % parquetVersion,
      "org.apache.hadoop"    % "hadoop-client"    % hadoopVersion exclude ("log4j", "log4j"),
      "org.scalatest"       %% "scalatest"        % scalaTestVersion % "test"
    )
  )

lazy val `stream-loader-iceberg` = project
  .in(file("stream-loader-iceberg"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop"  % "hadoop-common"   % hadoopVersion,
      "org.apache.iceberg" % "iceberg-core"    % icebergVersion,
      "org.apache.iceberg" % "iceberg-data"    % icebergVersion,
      "org.apache.iceberg" % "iceberg-parquet" % icebergVersion   % "test",
      "org.scalatest"     %% "scalatest"       % scalaTestVersion % "test"
    )
  )

lazy val `stream-loader-s3` = project
  .in(file("stream-loader-s3"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "s3"              % "2.26.25",
      "org.scalatest"         %% "scalatest"       % scalaTestVersion % "test",
      "com.amazonaws"          % "aws-java-sdk-s3" % "1.12.765"       % "test",
      "org.gaul"               % "s3proxy"         % "2.2.0"          % "test"
    )
  )

val verticaVersion = "24.3.0-0"

lazy val `stream-loader-vertica` = project
  .in(file("stream-loader-vertica"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.vertica.jdbc"   % "vertica-jdbc"    % verticaVersion        % "provided",
      "org.scalatest"     %% "scalatest"       % scalaTestVersion      % "test",
      "org.scalatestplus" %% "scalacheck-1-18" % scalaCheckTestVersion % "test",
      "org.scalacheck"    %% "scalacheck"      % scalaCheckVersion     % "test"
    )
  )

val duckdbVersion = "1.0.0"

lazy val packAndSplitJars =
  taskKey[(File, File)]("Runs pack and splits out the application jars from the external dependency jars")
lazy val dockerImage = settingKey[String]("Full docker image name")

val IntegrationTest = config("it").extend(Test)

lazy val `stream-loader-tests` = project
  .in(file("stream-loader-tests"))
  .dependsOn(`stream-loader-clickhouse`)
  .dependsOn(`stream-loader-hadoop`)
  .dependsOn(`stream-loader-iceberg`)
  .dependsOn(`stream-loader-s3`)
  .dependsOn(`stream-loader-vertica`)
  .enablePlugins(PackPlugin)
  .enablePlugins(DockerPlugin)
  .enablePlugins(BuildInfoPlugin)
  .configs(IntegrationTest)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe"                     % "config"                           % "1.4.3",
      "ch.qos.logback"                   % "logback-classic"                  % "1.5.6",
      "com.zaxxer"                       % "HikariCP"                         % "5.1.0",
      "org.apache.iceberg"               % "iceberg-parquet"                  % icebergVersion,
      "com.vertica.jdbc"                 % "vertica-jdbc"                     % verticaVersion,
      "org.scalacheck"                  %% "scalacheck"                       % scalaCheckVersion,
      "org.scalatest"                   %% "scalatest"                        % scalaTestVersion      % "test",
      "org.scalatestplus"               %% "scalacheck-1-18"                  % scalaCheckTestVersion % "test",
      "org.slf4j"                        % "log4j-over-slf4j"                 % "2.0.13"              % "test",
      "org.mandas"                       % "docker-client"                    % "7.0.8"               % "test",
      "org.jboss.resteasy"               % "resteasy-client"                  % "6.2.9.Final"         % "test",
      "com.fasterxml.jackson.jakarta.rs" % "jackson-jakarta-rs-json-provider" % "2.17.2"              % "test",
      "org.duckdb"                       % "duckdb_jdbc"                      % duckdbVersion         % "test"
    ),
    inConfig(IntegrationTest)(Defaults.testTasks),
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
      dockerImage,
      "duckdbVersion" -> duckdbVersion
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
        from("eclipse-temurin:21.0.2_13-jre")

        env("APP_CLASS_PATH" -> s"$lib/*")

        runRaw(
          "apt-get update && " +
            "apt-get install -y --no-install-recommends zstd liblzo2-dev && " +
            "apt-get clean && rm -rf /var/lib/apt/lists/*"
        )

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
    Test / testOptions ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow")
    ),
    IntegrationTest / test := (IntegrationTest / test).dependsOn(docker).value,
    IntegrationTest / testOnly := (IntegrationTest / testOnly).dependsOn(docker).evaluated,
    IntegrationTest / testOptions := Seq(
      sbt.Tests.Setup(cl =>
        cl.loadClass("org.slf4j.LoggerFactory")
          .getMethod("getLogger", cl.loadClass("java.lang.String"))
          .invoke(null, "ROOT") // Prevents slf4j replay warnings during tests
      ),
      Tests.Argument(TestFrameworks.ScalaTest, "-n", "org.scalatest.tags.Slow"),
      Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3", "-minSuccessfulTests", "10")
    )
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
  versionScheme := Some("early-semver"),
  libraryDependencySchemes ++= Seq(
    "com.github.luben" % "zstd-jni" % "early-semver" // "strict" by default
  ),
  publishMavenStyle := true,
  Test / publishArtifact := false,
  Test / testOptions ++= Seq(
    sbt.Tests.Setup(cl =>
      cl.loadClass("org.slf4j.LoggerFactory")
        .getMethod("getLogger", cl.loadClass("java.lang.String"))
        .invoke(null, "ROOT") // Prevents slf4j replay warnings during tests
    )
  ),
  publishTo := sonatypePublishToBundle.value,
  homepage := Some(url(gitRepoUrl)),
  scmInfo := Some(ScmInfo(url(gitRepoUrl), s"scm:git:git@github.com:$gitRepo.git"))
)

lazy val copyDocAssets = taskKey[File]("Copy unidoc resources")

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
    ghpagesRepository := file(s"/tmp/ghpages/${organization.value}/${name.value}/${version.value}")
  )
  .aggregate(
    `stream-loader-core`,
    `stream-loader-clickhouse`,
    `stream-loader-hadoop`,
    `stream-loader-iceberg`,
    `stream-loader-s3`,
    `stream-loader-vertica`,
    `stream-loader-tests`
  )
