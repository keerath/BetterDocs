package com.kodebeagle.actor

import com.kodebeagle.indexer.RepoFileNameInfo

sealed trait Message

case class RepoWork(repoFileNameInfo: RepoFileNameInfo, path: String, indexedTags: Set[String])

case class Work(work: List[RepoWork]) extends Message

case object Result extends Message
