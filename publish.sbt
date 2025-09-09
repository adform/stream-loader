ThisBuild / organization := "com.adform"
ThisBuild / organizationName := "Adform"
ThisBuild / organizationHomepage := Some(url("https://www.adform.com"))

import BuildSettings._

ThisBuild / startYear := Some(2020)
ThisBuild / description := "Stream Loader: load data from Kafka into multiple backends"
ThisBuild / homepage := Some(url(gitRepoUrl))

ThisBuild / licenses := List(
  "MPL-2.0" -> url("http://mozilla.org/MPL/2.0/")
)

ThisBuild / developers := List(
  Developer(
    id = "sauliusvl",
    name = "Saulius Valatka",
    email = "saulius.vl@gmail.com",
    url = url("https://github.com/sauliusvl")
  )
)

ThisBuild / scmInfo := Some(
  ScmInfo(url(gitRepoUrl), s"scm:git:git@github.com:$gitRepo.git")
)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true

// Use Central Portal instead of OSSRH
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}
