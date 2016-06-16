package com.kodebeagle.crawler

/**
  * Created by keerathj on 15/5/16.
  */

trait Extractor {

  def extractStars: Int

  def extractBranch: String
}