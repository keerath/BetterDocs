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

package com.kodebeagle.crawler

import java.io.File
import java.net.URL
import java.util.Calendar

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.GitHubRepoDownloader._
import com.kodebeagle.indexer.RepoFileNameInfo
import com.kodebeagle.logging.Logger
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.io.FileUtils
import org.eclipse.jgit.api.CloneCommand
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}

/**
 * This class relies on Github's {https://developer.github.com/v3/} Api.
 */
object GitHubApiHelper extends Logger {

  implicit val format = DefaultFormats
  private val client = new HttpClient()
  var token: String = KodeBeagleConfig.githubTokens(0)
  var retryCount: Int = 0

  /**
   * Access Github's
   * [[https://developer.github.com/v3/repos/#list-all-public-repositories List all repositories]]
 *
   * @param since Specify id of repo to start the listing from. (Pagination)
   */
  def getAllGitHubRepos(since: Int): (List[Map[String, String]], Int) = {
    val method = executeMethod(s"https://api.github.com/repositories?since=$since", token)
    val nextSinceValueRaw = Option(method.getResponseHeader("Link").getElements.toList(0).getValue)
    val nextSince = nextSinceValueRaw.get.substring(0, nextSinceValueRaw.get.length - 1).toInt
    val json = httpGetJson(method).toList
    // Here we can specify all the fields we need from repo query.
    val interestingFields = List("full_name", "fork")
    val allGitHubRepos = for {
      j <- json
      c <- j.children
      map = (for {
        JObject(child) <- c
        JField(name, value) <- child
        if interestingFields.contains(name)
      } yield name -> value.values.toString).toMap
    } yield (map)
    (allGitHubRepos, nextSince)
  }

  /**
   * Get repository details for an organization.
   * Access Github's
   * [[https://developer.github.com/v3/repos/#list-organization-repositories]]
   */
  def getAllGitHubReposForOrg(orgs: String, page: Int): List[RepoFileNameInfo] = {
    val method = executeMethod(s"https://api.github.com/orgs/$orgs/repos?page=$page", token)
    val json = httpGetJson(method).toList
    for (j <- json; c <- j.children) yield extractRepoInfo(c)
  }

  /**
   * Parallel fetch is not worth trying since github limits per user limit of 5000 Req/hr.
   */
  def fetchDetails(repoMap: Map[String, String]): Option[RepoFileNameInfo] = {
    for {
      repo <-
      httpGetJson(executeMethod("https://api.github.com/repos/" + repoMap("full_name"), token))
    } yield extractRepoInfo(repo)
  }

  def extractRepoInfo(repo: JValue): RepoFileNameInfo = {
    RepoFileNameInfo((repo \ "owner" \ "login").extract[String],
      (repo \ "id").extract[Int], (repo \ "name").extract[String], (repo \ "fork").extract[Boolean],
      (repo \ "language").extract[String], (repo \ "default_branch").extract[String],
      (repo \ "stargazers_count").extract[Int], "")
  }

  /*
   * Find the number of repo pages in an organisation.
   */
  def repoPagesCount(url: String): Int = executeMethod(url, token).getResponseHeader("Link").
    getElements.toList(1).getValue.substring(0, 2).replaceAll("\\W+", "").toInt

  /*
     * Helper for accessing Java - Apache Http client.
     * (It it important to stick with the current version and all.)
     */
  def httpGetJson(method: GetMethod): Option[JValue] = {
    val status = method.getStatusCode
    if (status == 200) {
      // ignored parsing errors if any, because we can not do anything about them anyway.
      Try(parse(method.getResponseBodyAsString)).toOption
    } else {
      log.error("Request failed with status:" + status + "Response:"
        + method.getResponseHeaders.mkString("\n") +
        "\nResponseBody " + method.getResponseBodyAsString)
      None
    }
  }

  def executeMethod(url: String, token: String): GetMethod = {
    val method = new GetMethod(url)
    method.setDoAuthentication(true)
    // Please add the oauth token instead of <token> here. Or github may give 403/401 as response.
    method.addRequestHeader("Authorization", s"token $token")
    log.debug(s"using token $token")
    client.executeMethod(method)
    val requestLimitRemaining = method.getResponseHeader("X-RateLimit-Remaining").getValue
    repoDownloader ! RateLimit(requestLimitRemaining)
    method
  }

  def createDirectoryWithDate(targetDir: String): String = {
    val cal = Calendar.getInstance()
    val (day, month, year) = (cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.MONTH),
      cal.get(Calendar.YEAR))
    val githubdir = targetDir + "/" + day + "_" + month + "_" + year
    if (!new File(githubdir).exists) { new File(githubdir).mkdirs }
    githubdir
  }

  def downloadRepository(r: RepoFileNameInfo, targetDir: String, retry: Int = 0): Option[File] = {
    if (retry < 3) {
      val githubdir: String = createDirectoryWithDate(targetDir)
      try {
        val repoFile = new File(
          githubdir +
            s"/repo~${r.login}~${r.name}~${r.id}~${r.fork}~${r.language}~${r.defaultBranch}" +
            s"~${r.stargazersCount}.zip")
        log.info(s"Downloading $repoFile")
        FileUtils.copyURLToFile(new
            URL(s"https://github.com/${r.login}/${r.name}/archive/${r.defaultBranch}.zip"),
          repoFile, 5000 * 12, 5000 * 12)
        Some(repoFile)
      } catch {
        case x: java.net.SocketTimeoutException =>
          log.info("Connection lost, retrying")
          retryCount += 1
          downloadRepository(r, targetDir, retryCount)
          None
        case x: java.io.FileNotFoundException =>
          log.error(s"File not found exception $r")
          None
        case x: java.io.IOException =>
          log.error(s"Failed to download $r")
          None
      }
    } else {
      log.info(s"Retry limit exceeded $r")
      None
    }
  }

  def cloneRepository(r: RepoFileNameInfo, url: String, targetDir: String): Unit = {
    val githubdir = createDirectoryWithDate(targetDir);
    val filePath =  githubdir +
      s"/repo~${r.login}~${r.name}~${r.id}~${r.fork}~${r.language}~${r.defaultBranch}" +
      s"~${r.stargazersCount}"
    log.info(s"Downloading $filePath")
    try {
      val file = new File(filePath)
      val clone = new CloneCommand()
      clone.setBare(false)
      clone.setCloneAllBranches(false)
      clone.setDirectory(file).setURI(url)
      clone.call()
      GitHubRepoDownloader.zipActor ! filePath
    } catch {
      case x: Throwable =>
        log.error(s"Failed to download $url", x)
    }
  }
}

object GitHubRepoCrawlerApp {

  import com.kodebeagle.crawler.GitHubApiHelper._

  def main(args: Array[String]): Unit = {

    Try(args(0).toInt) match {

      case Success(since) =>
        log.info(s"Downloading repo since : $since")
        repoDownloader ! DownloadPublicRepos(since, args(1))

      case Failure(ex: NumberFormatException) if !args(0).isEmpty =>
        log.info(s"Downloading repo for organisation:" + args(0))
        repoDownloader ! DownloadOrganisationRepos(args(0))

    }
  }

  def downloadFromOrganization(organizationName: String): Unit = {
    val pageCount = repoPagesCount(s"https://api.github.com/orgs/$organizationName/repos")
    log.info("page count :" + pageCount)
    (1 to pageCount) foreach { page =>
      getAllGitHubReposForOrg(organizationName, page).filter(x => !x.fork && x.language == "Java")
        .map(x => cloneRepository(x,s"https://github.com/${x.login}/${x.name}",
        KodeBeagleConfig.githubDir))
    }
  }

  def downloadFromRepoIdRange(since: Int, zipOrClone: String): Int = {
    val (allGithubRepos, next) = getAllGitHubRepos(since)
    allGithubRepos.filter(x => x("fork") == "false").distinct
      .flatMap(fetchDetails).distinct.filter(x => x.language == "Java" && !x.fork)
      .map { x =>
      if (zipOrClone.equalsIgnoreCase("clone")) {
        cloneRepository(x, s"https://github.com/${x.login}/${x.name}",
          KodeBeagleConfig.githubDir)
     } else {
        retryCount = 0
        downloadRepository(x, KodeBeagleConfig.githubDir)
      }
    }
    next
  }
}
