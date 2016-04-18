package com.kodebeagle.actor

import java.io.{File, FileFilter}
import java.net.URI

import akka.actor.Actor
import com.kodebeagle.logging.Logger
import com.kodebeagle.util.{GitHelper, UnzipUtil}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class ZipTagWorker(batch: String) extends Actor with Logger {

  private val TMPDIR = System.getProperty("java.io.tmpdir")
  private val SEP = File.separator
  private val masterRef = context.actorSelection("akka://ZipForEachTag/user/ZipTagMaster")
  private[actor] val ZIPEXTRACTDIR = s"$TMPDIR${SEP}extractedzips"
  private[actor] val ZIPSDIR = s"$TMPDIR${SEP}zips"
  new File(ZIPSDIR).mkdir()

  override def receive: Receive = {
    case repoWork: RepoWork => doWork(repoWork)
    case _ =>
  }

  private def doWork(repoWork: RepoWork) = {
    log.info(s"Extracting repository ${repoWork.path}")
    Future {
      extractRepo(repoWork.path)
    }.onComplete {
      case Success(repoDir) =>
        implicit val gitHelper =
          new GitHelper(repoDir.getAbsolutePath)
        gitHelper.stashChanges()
        val tagsToIndex = gitHelper.getTagsToIndex(repoWork.indexedTags)
        log.info(s"Creating zip per tag for repository ${repoWork.path}")
        Future {
          tagsToIndex.foreach(tag =>
            uploadZip(tag, repoDir.getName))
        }.onComplete {
          case Success(_) => FileUtils.forceDelete(repoDir)
            masterRef ! Result
          case Failure(ex) => ex.printStackTrace()
        }
      case Failure(ex) => ex.printStackTrace()
    }
  }

  private def replaceSlash(tag: String) = tag.replaceAll("/", "kbslash12")

  private def createZip(zipPath: String, repoDirName: String) = {
    import scala.sys.process._
    Process(Seq("zip", "-r", zipPath, repoDirName), new File(ZIPEXTRACTDIR)).!!
  }

  private def uploadZip(tag: String, repoDirName: String)
                       (implicit gitHelper: GitHelper) = {
    gitHelper.checkoutTag(tag)
    val cleanedTag = replaceSlash(tag)
    val zipName = s"$repoDirName~$cleanedTag"
    val zipPath = s"$ZIPSDIR$SEP$zipName.zip"
    val repoPath = s"$ZIPEXTRACTDIR$SEP$repoDirName"
    val trimmedRepoPath = s"$repoPath~$cleanedTag"
    FileUtils.copyDirectory(new File(repoPath), new File(trimmedRepoPath), new FileFilter {
      override def accept(file: File): Boolean = {
        val name = file.getName
        name.endsWith(".java") || name.endsWith(".scala") || file.isDirectory
      }
    })
    createZip(zipPath, trimmedRepoPath.stripPrefix(s"$ZIPEXTRACTDIR$SEP"))
    log.info(s"Uploading $zipPath to HDFS")
    moveToHDFS(zipPath, batch)
    log.info(s"Uploaded $zipPath to HDFS")
    FileUtils.forceDelete(new File(trimmedRepoPath))
  }

  private def moveToHDFS(zipPath: String, batch: String) = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val hdfs = FileSystem.get(URI.create("hdfs://192.168.2.145:9000"), conf)
    hdfs.moveFromLocalFile(new Path(zipPath), new Path(s"/user/zips$batch"))
  }

  private def extractRepo(path: String) = {
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
