/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.{GitBranchPrompt, GitVersioning}
import de.johoop.cpd4sbt.CopyPasteDetector._
import de.johoop.cpd4sbt.Language
import de.johoop.findbugs4sbt.FindBugs._
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.{AssemblyPlugin, MergeStrategy}

// scalastyle:off
object KodeBeagleBuild extends Build {

  lazy val root = Project(
    id = "kodebeagle",
    base = file("."),
    settings = kodebeagleSettings,
    aggregate = aggregatedProjects
  ).enablePlugins(GitVersioning).enablePlugins(GitBranchPrompt)

  val excludeDependency = Seq(assemblyOption in assembly := (assemblyOption in assembly).value.copy(
    includeScala = false, includeDependency = false))

  lazy val core = Project("core", file("core"), settings =
    coreSettings)

  lazy val fatJar = Project("fatJar", file("core"), settings =
    fatJarSettings)

  lazy val pluginImpl = Project("pluginImpl", file("plugins/idea/pluginImpl"), settings =
    pluginSettingsFull ++ findbugsSettings ++ scalaPluginSettings ++
      codequality.CodeQualityPlugin.Settings ++ excludeDependency).settings(
    findbugsExcludeFilters := Some(
      <FindBugsFilter>
        <Match>
          <Class name="~.*PsiScalaElementVisitor.*"/>
        </Match>
      </FindBugsFilter>
    )
  ) dependsOn pluginBase

  lazy val pluginTests = Project("pluginTests", file("plugins/idea/pluginTests"), settings =
    pluginTestSettings) dependsOn pluginImpl disablePlugins AssemblyPlugin

  lazy val pluginBase = Project("pluginBase", file("plugins/idea/pluginBase"), settings =
    pluginSettings ++ findbugsSettings ++
      codequality.CodeQualityPlugin.Settings)

  val scalacOptionsList = Seq("-encoding", "UTF-8", "-unchecked", "-optimize", "-deprecation",
    "-feature")

  // This is required for plugin devlopment.
  val ideaLib = sys.env.get("IDEA_LIB").orElse(sys.props.get("idea.lib"))


  def aggregatedProjects: Seq[ProjectReference] = {
    if (ideaLib.isDefined) {
      Seq(core, fatJar, pluginBase, pluginImpl, pluginTests)
    } else {
      println(
        """[warn] Plugin project disabled. To enable append -Didea.lib="idea/lib"""" ++
          """ to JVM params in SBT settings or""" ++
          """ while invoking sbt (incase it is called from commandline.). """)
      Seq(core)
    }
  }

  def scalaPluginSettings = Seq(
    scalaVersion := "2.10.4",
    libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value % "provided"
  )

  def pluginSettings = kodebeagleSettings ++ (if (ideaLib.isEmpty) Seq()
  else
    cpdSettings ++ Seq(
      name := "KodeBeagleIdeaPlugin",
      libraryDependencies ++= Dependencies.ideaPlugin,
      autoScalaLibrary := false,
      cpdLanguage := Language.Java,
      cpdMinimumTokens := 30,
      unmanagedBase := file(ideaLib.get)
    ))

  def pluginSettingsFull = pluginSettings ++ (if (ideaLib.isEmpty) Seq()
  else Seq(
    resourceGenerators in Compile <+=
      (resourceDirectory in Compile, version, git.gitCurrentTags) map { (dir, v, tags) =>
        val file = dir / "META-INF" / "plugin.xml"
        IO.write(file, IO.read(file).replaceAll("VERSION_STRING", v))
        Seq(file)
      }))

  def pluginTestSettings = pluginSettings ++ Seq(
    name := "plugin-test",
    libraryDependencies ++= Dependencies.ideaPluginTest,
    autoScalaLibrary := true,
    scalaVersion := "2.10.4")

  def coreSettings = kodebeagleSettings ++ Seq(libraryDependencies ++= Dependencies.kodebeagle)++ Seq(jacoco.settings:_*)

  def fatJarSettings = kodebeagleSettings ++ Seq(libraryDependencies ++= Dependencies.kodebeagleProvided) ++ Seq(assemblyMergeStrategy in assembly := {
    case "plugin.properties" | "plugin.xml" | ".api_description" | "META-INF/eclipse.inf" | ".options" => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }) ++ Seq(target := baseDirectory.value / "assembly")

  def kodebeagleSettings =
    Defaults.coreDefaultSettings ++ Seq(
      name := "KodeBeagle",
      organization := "com.kodebeagle",
      git.baseVersion := "0.1.0",
      scalaVersion := "2.10.4",
      git.useGitDescribe := true,
      scalacOptions := scalacOptionsList,
      //resolvers += Resolver.mavenLocal,
      resolvers += Resolver.url("http://gitblit.github.io/gitblit-maven/"),
      updateOptions := updateOptions.value.withCachedResolution(true),
      updateOptions := updateOptions.value.withLatestSnapshots(false),
      crossPaths := false,
      fork := true,
      javacOptions ++= Seq("-source", "1.7"),
      javaOptions += "-Xmx6048m",
      javaOptions += "-XX:+HeapDumpOnOutOfMemoryError",
      parallelExecution in jacoco.Config := false
    )
}

object Dependencies {

  val sparkCore = "org.apache.spark" %% "spark-core" % "1.6.1"
  val sparkSql = "org.apache.spark" %% "spark-sql" % "1.6.1"
  val graphx = "org.apache.spark" %% "spark-graphx" % "1.4.1"

  val spark = Seq(sparkCore, sparkSql, graphx)
  val sparkProvided = spark.map(d => d % "provided")

  // val esSpark = "org.elasticsearch" %% "elasticsearch-spark" % "2.1.0.Beta4"
  val esSpark = "org.elasticsearch" % "elasticsearch-spark_2.10" % "5.0.0-alpha4"
  val esSparkExcluded = esSpark.exclude("org.apache.spark", "spark-sql_2.10")
    .exclude("org.apache.spark", "spark-core_2.10")

  val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "3.7.0.201502260915-r"
  val jgitProvided = jgit % "provided"

  val scalastyle = "org.scalastyle" %% "scalastyle" % "0.7.0"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  val slf4j = "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  val json4s = "org.json4s" %% "json4s-ast" % "3.2.10"
  val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.2.10"
  val httpClient = "commons-httpclient" % "commons-httpclient" % "3.1"
  val config = "com.typesafe" % "config" % "1.2.1"
  val commonsIO = "commons-io" % "commons-io" % "2.4"

  val guava = "com.google.guava" % "guava" % "18.0"
  val akka = "com.typesafe.akka" %% "akka-actor" % "2.3.15"
  val compress = "org.apache.commons" % "commons-compress" % "1.10"

  val junit = "junit" % "junit" % "4.12"
  val rhino = "org.mozilla" % "rhino" % "1.7R4"

  // val gitblit = ("com.gitblit" % "gitblit" % "1.8.0").intransitive()

  //Eclipse dependencies for Tassal libs
  object EclipseDeps {
    val tycho = "org.eclipse.tycho" % "org.eclipse.jdt.core" % "3.10.0.v20140604-1726"
    val contentType = "org.eclipse.birt.runtime" % "org.eclipse.core.contenttype" % "3.4.200.v20130326-1255"
    val coreJobs = "org.eclipse.birt.runtime" % "org.eclipse.core.jobs" % "3.5.300.v20130429-1813"
    val coreResources = "org.eclipse.birt.runtime" % "org.eclipse.core.resources" % "3.8.101.v20130717-0806"
    val coreRT = "org.eclipse.birt.runtime" % "org.eclipse.core.runtime" % "3.9.0.v20130326-1255"
    val eqCommon = "org.eclipse.birt.runtime" % "org.eclipse.equinox.common" % "3.6.200.v20130402-1505"
    val eqPref = "org.eclipse.birt.runtime" % "org.eclipse.equinox.preferences" % "3.5.100.v20130422-1538"
    val eqReg = "org.eclipse.birt.runtime" % "org.eclipse.equinox.registry" % "3.5.301.v20130717-1549"
    val osgi = "org.eclipse.birt.runtime" % "org.eclipse.osgi" % "3.9.1.v20130814-1242"
    val text = "org.eclipse.text" % "org.eclipse.text" % "3.5.101"

    val allDeps = Seq(tycho, contentType, coreJobs, coreResources, coreRT, eqCommon, eqPref, eqReg, osgi, text)
    val allDepsInTransitive = allDeps.map(d => d.intransitive())
  }

  val base = Seq(akka, httpClient, scalastyle, scalaTest, slf4j, json4s, config, json4sJackson, commonsIO,
    guava, compress, junit, rhino, jgit)

  val kodebeagle = base ++ EclipseDeps.allDeps ++ spark ++ Seq(esSpark)

  val kodebeagleProvided =
    base ++ EclipseDeps.allDepsInTransitive ++ sparkProvided ++ Seq(esSparkExcluded)

  val ideaPluginTest = Seq(scalaTest, commonsIO)
  val ideaPlugin = Seq()
  // transitively uses
  // commons-compress-1.4.1
}

// scalastyle:on
