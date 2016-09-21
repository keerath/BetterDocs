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

package com.kodebeagle.model

import com.kodebeagle.indexer._
import com.kodebeagle.javaparser.JavaASTParser.ParseType
import com.kodebeagle.javaparser.{JavaASTParser, SingleClassBindingResolver}
import com.kodebeagle.logging.Logger
import com.kodebeagle.model.GithubRepo.GithubRepoInfo
import org.eclipse.jdt.core.dom.CompilationUnit

import scala.util.Try

class JavaRepo(val baseRepo: GithubRepo) extends Repo with Logger
  with LazyLoadSupport {

  // TODO: This constants needs to go somewhere else
  private val JAVA_LANGUAGE = "java"

  override def files: Iterator[JavaFileInfo] = baseRepo.files
    .filter(_.extractLang().equalsIgnoreCase("java")).map(new JavaFileInfo(_, this))


  def summary: JavaRepoSummary = {
    val agg = baseRepo.gitLogAggregation
    JavaRepoSummary(GithubRepo.remote, baseRepo.repoInfo.get,
      GitHistory(agg.mostChangedFiles(10).map(_._1), agg.allCommits.map(_.commit).toList))
  }
}

class JavaFileInfo(val baseFile: GithubFileInfo, repo: JavaRepo) extends FileInfo
  with LazyLoadSupport with Logger {

  assert(baseFile.fileName.endsWith(".java"),
    s"A java file is expected. Actual file: ${baseFile.fileName}")

  private var _searchableRefs: Option[TypeReference] = None

  private var _fileMetaData: Option[FileMetaData] = None

  private var _imports: Option[Set[String]] = None

  private var _repoPath: Option[String] = None

  private var _typesInFile: Option[TypesInFile] = None

  private var _javaDoc: Option[Set[TypeDocsIndices]] = None

  def searchableRefs: TypeReference = {
    getOrCompute(_searchableRefs, () => {
      parse()
      _searchableRefs.get
    })
  }

  def fileMetaData: FileMetaData = {
    getOrCompute(_fileMetaData, () => {
      parse()
      _fileMetaData.get
    })
  }

  def imports: Set[String] = {
    getOrCompute(_imports, () => {
      parse()
      _imports.get
    })
  }

  def repoPath: String = {
    getOrCompute(_repoPath, () => {
      parse()
      _repoPath.get
    })
  }

  def typesInFile: TypesInFile = {
    getOrCompute(_typesInFile, () => {
      parse()
      _typesInFile.get
    })
  }

  def javaDocs: Set[TypeDocsIndices] = {
    getOrCompute(_javaDoc, () => {
      parse()
      _javaDoc.get
    })
  }

  def fileDetails: FileDetails = {
    // TODO: Revisit #600 and explain why it happens
    val agg = repo.baseRepo.gitLogAggregation
    val file = baseFile.filePath
    val commitCount = agg.fileCommmitCount(file)
    val topAuthors = agg.topAuthors(file, 5).map(_._1)
    val cooccuring = agg.coOccuringFiles(file, 10).map(_._1)
    FileDetails(repoFileLocation, commitCount, topAuthors, cooccuring)
  }

  override def fileName: String = baseFile.fileName

  override def sloc: Int = baseFile.sloc

  override def fileContent: String = baseFile.fileContent

  override def language: String = baseFile.language

  override def repoId: Long = baseFile.repoId

  override def repoFileLocation: String = baseFile.repoFileLocation

  def isTestFile(): Boolean = imports.exists(_.contains("org.junit"))

  // Reset the generated indices to None so that the older strings can be GCed
  def free(): Unit = {
    _repoPath = None
    _searchableRefs = None
    _fileMetaData = None
    _imports = None
    _javaDoc = None
    _typesInFile = None
  }

  /**
    * This method parses the java file and updates all that needs to be exposed by this class.
    *
    * The method is called lazily on when the data depending on parse is first requested, and
    * then all such data is computed and stored in the fields of the class.
    *
    * @return
    */
  private def parse() = {
    import JavaFileInfo._

    import scala.collection.JavaConversions._

    val parser: JavaASTParser = new JavaASTParser(true, true)
    _repoPath = Option(s"${baseFile.githubRepoInfo.login}/${baseFile.githubRepoInfo.name}")

    // The file may not even be well formed, so the parser may throw an
    // IllegalArgumentException. Need to handle such a case.
    val cu: Option[CompilationUnit] = Try {
      parser.getAST(fileContent, ParseType.COMPILATION_UNIT)
        .asInstanceOf[CompilationUnit]
    }.toOption

    if (cu.isEmpty) {
      log.error(s"Compilation unit is null for $fileName")
      _imports = _emptyImports
      _searchableRefs = emptySearchableRefs(repoFileLocation)
      _fileMetaData = emptyFileMetadata(repoId, repoFileLocation)
      _typesInFile = emptyTypesInFile(repoPath, repoFileLocation)
      _javaDoc = _emptyJavaDocs

    } else {
      val scbr: SingleClassBindingResolver = new SingleClassBindingResolver(cu.get)
      scbr.resolve()

      val nodeVsType = scbr.getTypesAtPosition
      val score = baseFile.githubRepoInfo.stargazersCount
      val externalTypeRefs = ExternalRefsIndexHelper.extractTypeReference(scbr, cu.get,
        score, repoFileLocation)
      val fileMetaData = FileMetaDataIndexHelper.generateMetaData(scbr, cu.get,
        repoId, repoFileLocation)

      _imports = Option(scbr.getImports.toSet)
      _searchableRefs = Option(externalTypeRefs)
      _fileMetaData = Option(fileMetaData)
      _typesInFile = Option(TypesInFile(repoPath, repoFileLocation,
        TypesInFileIndexHelper.usedTypesInFile(scbr),
        TypesInFileIndexHelper.declaredTypesInFile(scbr)))
      _javaDoc = Option(JavaDocIndexHelper.generateJavaDocs(repoId, repoFileLocation, scbr))
    }

  }
}

case class JavaRepoSummary(remote: String, gitHubInfo: GithubRepoInfo, gitHistory: GitHistory)

case class GitHistory(mostChanged: List[String], commits: List[Commit])

case class FileDetails(file: String, commits: List[Commit], topAuthors: List[String],
                       coChange: List[String])

case class JavaRepoStatistics(sloc: Int, fileCount: Int, size: Long) extends RepoStatistics


object JavaFileInfo {

  val _emptyContextSet = Set.empty[Context]
  val _emptyPayLoadTypeSet = Set.empty[PayloadType]
  val _emptyTypeDeclarationList = List.empty[TypeDeclaration]
  val _emptyExtRefList = List.empty[ExternalRef]
  val _emptyInternalRefList = List.empty[InternalRef]
  val _emptyMethodTypeLocList = List.empty[MethodTypeLocation]
  val _emptyMethodDefLocList = List.empty[MethodDefinition]
  val _emptyDeclaredTypeMap = Map.empty[String, Set[MethodType]]
  val _emptyUsedTypesMap = Map.empty[String, (Set[String], Set[MethodType])]
  val _emptyImports: Option[Set[String]] = Option(Set.empty)
  val _emptySuperTypes = SuperTypes(Map.empty, Map.empty)
  val _emptyJavaDocs: Option[Set[TypeDocsIndices]] = Option(Set.empty)

  def emptySearchableRefs(repoFileLocation: String): Option[TypeReference] =
    Option(TypeReference(_emptyContextSet, Payload(_emptyPayLoadTypeSet,
      0L, repoFileLocation), 0L, repoFileLocation))

  def emptyFileMetadata(repoId: Long, repoFileLocation: String): Option[FileMetaData] =
    Option(FileMetaData(repoId, repoFileLocation, _emptySuperTypes, _emptyTypeDeclarationList,
      _emptyExtRefList, _emptyMethodDefLocList, _emptyInternalRefList))

  def emptyTypesInFile(repoPath: String, repoFileLocation: String): Option[TypesInFile] =
    Option(TypesInFile(repoPath, repoFileLocation, _emptyUsedTypesMap, _emptyDeclaredTypeMap))

}

