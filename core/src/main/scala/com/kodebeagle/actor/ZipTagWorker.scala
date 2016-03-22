package com.kodebeagle.actor

import java.io.File

import akka.actor.Actor
import com.kodebeagle.crawler.ZipUtil
import com.kodebeagle.indexer.RepoFileNameInfo
import com.kodebeagle.logging.Logger
import com.kodebeagle.util.{GitHelper, UnzipUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import sys.process._

import scala.util.Try

class ZipTagWorker extends Actor with Logger {

  private val TMPDIR = System.getProperty("java.io.tmpdir")
  private val SEP = File.separator
  private[actor] val ZIPEXTRACTDIR = s"$TMPDIR${SEP}extractedzips"
  private[actor] val ZIPSDIR = s"$TMPDIR${SEP}zips"
  new File(ZIPSDIR).mkdir()

  override def receive: Receive = {
    case Work(listOfRepoWork) =>
      sender() ! listOfRepoWork.foreach(doWork)
      Result
  }

  private def doWork(repoWork: RepoWork) = {
    log.info(s"Extracting repository ${repoWork.path}")
    val repoDir = extractRepo(repoWork.repoFileNameInfo, repoWork.path)
    implicit val gitHelper = new GitHelper(repoDir.getAbsolutePath)
    gitHelper.stashChanges()
    val tagsToBeIndexed = gitHelper.getTagsToIndex(repoWork.indexedTags)
    log.info(s"Creating zip per tag for repository ${repoWork.path}")
    val x = repoDir.getAbsolutePath
    val paths = tagsToBeIndexed.map(tag => createZipForTag(tag, repoDir.getName)).toArray
    moveToHdfs(paths)
    s"rm -fr ${repoDir.getAbsolutePath}" !
  }

  private def createZipForTag(tag: String, repoDirName: String)
                             (implicit gitHelper: GitHelper) = {
    gitHelper.checkoutTag(tag)
    val zipName = s"$repoDirName~$tag"
    val zipPath = s"$ZIPSDIR$SEP$zipName.zip"
    val repoPath = s"$ZIPEXTRACTDIR$SEP$repoDirName"
    ZipUtil.createZip(repoPath, zipPath)
    new Path(zipPath)
  }

  private def moveToHdfs(paths: Array[Path]) = {
    val conf = new Configuration()
    conf.set("fs.default.name", "hdfs://192.168.2.145:9000")
    val fs = FileSystem.get(conf)
    Try(fs.moveFromLocalFile(paths, new Path("/user/zips")))
  }

  private def extractRepo(repoFileNameInfo: RepoFileNameInfo, path: String) = {
    unzipFile(path, ZIPEXTRACTDIR)
    new File(s"$ZIPEXTRACTDIR$SEP${getRepoDir(path)}")
  }

  private def getRepoDir(path: String) = {
    val indexOfSlash = path.lastIndexOf(SEP)
    val indexOfDot = path.lastIndexOf('.')
    path.substring(indexOfSlash + 1, indexOfDot)
  }

  private def unzipFile(zipPath: String, destDir: String) = {
    val unzipUtil = UnzipUtil.getInstance()
    Try(unzipUtil.unzip(zipPath, destDir))
  }
}
