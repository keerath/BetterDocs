logLevel := Level.Warn

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "3.0.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.7.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.2")

addSbtPlugin("de.johoop" % "findbugs4sbt" % "1.4.0")

addSbtPlugin("de.johoop" % "cpd4sbt" % "1.1.5")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.4")

resolvers += "corux-releases" at "http://tomcat.corux.de/nexus/content/repositories/releases/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

