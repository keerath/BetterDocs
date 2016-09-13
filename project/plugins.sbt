logLevel := Level.Warn

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "3.0.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

addSbtPlugin("de.johoop" % "findbugs4sbt" % "1.4.0")

addSbtPlugin("de.johoop" % "cpd4sbt" % "1.1.5")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.5")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")

addSbtPlugin("de.johoop" % "jacoco4sbt" % "2.2.0")

resolvers += "corux-releases" at "http://tomcat.corux.de/nexus/content/repositories/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += Resolver.url("scoverage-bintray",
  url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(

  "net.sourceforge.pmd" % "pmd" % "5.1.3" exclude("org.ow2.asm", "asm")
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

