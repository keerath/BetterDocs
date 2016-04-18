package com.kodebeagle.util

import java.io.File

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.storage.file.FileRepositoryBuilder

import scala.collection.JavaConversions._

class GitHelper(gitRepoPath: String) {

  private val repository = buildRepository(gitRepoPath)
  private val git = new Git(repository)
  private val defaultBranch = repository.getBranch

  private def buildRepository(repoPath: String) = {
    val repositoryBuilder = new FileRepositoryBuilder
    repositoryBuilder.setMustExist(true).findGitDir(new File(repoPath)).build()
  }

  def stashChanges(): Unit = {
    git.stashCreate().setIncludeUntracked(true).call()
  }

  def checkoutTag(tag: String): Unit = {
    if (tag == "HEAD") {
      git.checkout().setForce(true).setName(defaultBranch).call()
    } else {
      git.checkout.setForce(true).setName(tag).call()
    }
  }

  def getTagsToIndex(indexedTags: Set[String]): Set[String] = {
    if (indexedTags.isEmpty) {
      getGitTags + "HEAD"
    } else {
      getGitTags.diff(indexedTags.filterNot(_ == "HEAD"))
    }
  }

  private def getGitTags = repository.getTags.map(_._1).toSet
}
