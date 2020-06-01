name := "stream-loader"

ThisBuild / organization := "com.adform"
ThisBuild / organizationName := "Adform"
ThisBuild / scalaVersion := "2.13.2"
ThisBuild / scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.8")

ThisBuild / startYear := Some(2020)
ThisBuild / licenses += ("MPL-2.0", new URL("http://mozilla.org/MPL/2.0/"))

ThisBuild / developers := List(
  Developer("sauliusvl", "Saulius Valatka", "saulius.vl@gmail.com", url("https://github.com/sauliusvl"))
)

enablePlugins(GitVersioning)
ThisBuild / git.useGitDescribe := true

val gitRepo = "git@github.com:adform/stream-loader.git"
val gitRepoUrl = "https://github.com/adform/stream-loader"

val scalaTestVersion = "3.1.2"
val scalaCheckVersion = "1.14.3"
val scalaCheckTestVersion = "3.1.2.0"

lazy val `stream-loader-core` = project
  .in(file("stream-loader-core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    buildInfoPackage := s"${organization.value}.streamloader",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, git.gitHeadCommit),
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-reflect"     % scalaVersion.value,
      "org.apache.kafka"  % "kafka-clients"     % "2.5.0",
      "org.log4s"         %% "log4s"            % "1.8.2",
      "org.anarres.lzo"   % "lzo-commons"       % "1.0.6",
      "org.xerial.snappy" % "snappy-java"       % "1.1.7.5",
      "org.lz4"           % "lz4-java"          % "1.7.1",
      "com.github.luben"  % "zstd-jni"          % "1.4.5-2" classifier "linux_amd64",
      "com.univocity"     % "univocity-parsers" % "2.8.4",
      "org.json4s"        %% "json4s-native"    % "3.6.8",
      "io.micrometer"     % "micrometer-core"   % "1.5.1",
      "org.scalatest"     %% "scalatest"        % scalaTestVersion % "test",
      "org.scalatestplus" %% "scalacheck-1-14"  % scalaCheckTestVersion % "test",
      "org.scalacheck"    %% "scalacheck"       % scalaCheckVersion % "test",
      "ch.qos.logback"    % "logback-classic"   % "1.2.3" % "test"
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
    libraryDependencies ++= Seq(
      "ru.yandex.clickhouse" % "clickhouse-jdbc"  % "0.2.4",
      "org.scalatest"        %% "scalatest"       % scalaTestVersion % "test",
      "org.scalatestplus"    %% "scalacheck-1-14" % scalaCheckTestVersion % "test",
      "org.scalacheck"       %% "scalacheck"      % scalaCheckVersion % "test"
    )
  )

val parquetVersion = "1.11.0"

lazy val `stream-loader-hadoop` = project
  .in(file("stream-loader-hadoop"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.sksamuel.avro4s" %% "avro4s-core"     % "3.1.1",
      "org.apache.parquet"  % "parquet-avro"     % parquetVersion,
      "org.apache.parquet"  % "parquet-protobuf" % parquetVersion,
      "org.apache.hadoop"   % "hadoop-client"    % "3.2.1" exclude ("log4j", "log4j"),
      "org.scalatest"       %% "scalatest"       % scalaTestVersion % "test"
    )
  )

lazy val `stream-loader-s3` = project
  .in(file("stream-loader-s3"))
  .dependsOn(`stream-loader-core` % "compile->compile;test->test")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "software.amazon.awssdk" % "s3"              % "2.13.26",
      "org.scalatest"          %% "scalatest"      % scalaTestVersion % "test",
      "com.amazonaws"          % "aws-java-sdk-s3" % "1.11.792" % "test",
      "org.gaul"               % "s3proxy"         % "1.7.1" % "test",
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
      "org.scalatestplus" %% "scalacheck-1-14" % scalaCheckTestVersion               % "test",
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
      "com.typesafe"      % "config"           % "1.4.0",
      "ch.qos.logback"    % "logback-classic"  % "1.2.3",
      "com.zaxxer"        % "HikariCP"         % "3.4.5",
      "com.vertica"       % "vertica-jdbc"     % verticaVersion from verticaJarUrl,
      "org.scalacheck"    %% "scalacheck"      % scalaCheckVersion,
      "org.scalatest"     %% "scalatest"       % scalaTestVersion % "test,it",
      "org.scalatestplus" %% "scalacheck-1-14" % scalaCheckTestVersion % "test,it",
      ("com.spotify"      % "docker-client"    % "8.16.0" classifier "shaded") % "it"
    ),
    test := {}, // only integration tests present
    publish := {},
    publishLocal := {},
    skip in publish := true,
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
      val libDir = pack.value / "lib"
      val appLibDir = pack.value / "app-lib"
      appLibDir.mkdirs()
      IO.move(libDir / mainJar, appLibDir / mainJar)
      (libDir, appLibDir)
    },
    dockerImage := s"adform/${name.value}:${version.value}",
    dockerfile in docker := {

      val (depLib, appLib) = packAndSplitJars.value
      val lib = s"/opt/${name.value}/lib"
      val bin = s"/opt/${name.value}/bin/"

      new Dockerfile {
        from("adoptopenjdk:11.0.7_10-jre-hotspot")

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
    imageNames in docker := {
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
    test in IntegrationTest := (test in IntegrationTest).dependsOn(docker).value,
    testOnly in IntegrationTest := (testOnly in IntegrationTest).dependsOn(docker).evaluated,
    // Prevents slf4j replay warnings during tests
    testOptions in IntegrationTest ++= Seq(
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
  publishArtifact in Test := false,
  publishTo := sonatypePublishToBundle.value,

  homepage := Some(url(gitRepoUrl)),
  scmInfo := Some(ScmInfo(url(gitRepoUrl), s"scm:git:$gitRepo"))
)

lazy val copyDocAssets = taskKey[File]("Copy unidoc resources")

import com.typesafe.sbt.SbtGit.GitKeys._

lazy val `stream-loader` = project
  .in(file("."))
  .enablePlugins(ScalaUnidocPlugin)
  .enablePlugins(GhpagesPlugin)
  .settings(
    publish := {},
    publishLocal := {},
    skip in publish := true,
    scalacOptions in (Compile, doc) ++= Seq("-doc-title", name.value),
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(`stream-loader-tests`),
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
    makeSite := makeSite.dependsOn(unidoc in Compile).value,
    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
    siteSubdirName in ScalaUnidoc := "",
    mappings in makeSite ++= {
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
