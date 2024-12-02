ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.11.0")

addSbtPlugin("com.github.sbt" % "sbt-git" % "2.1.0")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.20")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.1")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")

addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")

libraryDependencies += "net.sourceforge.plantuml" % "plantuml" % "1.2024.8"

addSbtPlugin("com.github.sbt" % "sbt-ghpages" % "0.8.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.0")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
