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

// To be removed in the next intellij-scala plugin update

package com.imaginea.kodebeagle.impl.highlightfix

import com.imaginea.kodebeagle.impl.ui.MainWindow
import com.intellij.codeInsight.daemon.ProblemHighlightFilter
import com.intellij.ide.scratch.ScratchFileType
import com.intellij.openapi.application.ApplicationInfo
import com.intellij.openapi.roots.{JavaProjectRootsUtil, ProjectRootManager}
import com.intellij.openapi.vfs.VirtualFile
import com.intellij.psi.PsiFile
import org.jetbrains.plugins.scala.ScalaFileType

// scalastyle:off
class ScalaProblemHighlightFilter extends ProblemHighlightFilter {
  private val BUILD = 143

  def shouldHighlight(file: PsiFile): Boolean = {
    if (MainWindow.scalaPluginInstalledAndEnabled()) {
      val result = file.getFileType != ScalaFileType.SCALA_FILE_TYPE ||
        !JavaProjectRootsUtil.isOutsideJavaSourceRoot(file) ||
        (file.getVirtualFile != null && file.getVirtualFile.getExtension == "sc")
      /* Versions Of Intellij prior to build 143 don't have the class
       ScratchFileType and method getFileType on FileViewProvider  */
      if (ApplicationInfo.getInstance().getBuild.getBuildNumber >= BUILD) {
        result || (file.getViewProvider.getFileType == ScratchFileType.INSTANCE)
      } else {
        result
      }
    } else {
      false
    }
  }

  override def shouldProcessInBatch(file: PsiFile): Boolean = {
    if (MainWindow.scalaPluginInstalledAndEnabled()) {
      if (ProblemHighlightFilter.shouldHighlightFile(file)) {
        if (file.getFileType == ScalaFileType.SCALA_FILE_TYPE) {
          val vFile: VirtualFile = file.getVirtualFile
          if (vFile != null && ProjectRootManager.getInstance(file.getProject)
            .getFileIndex.isInLibrarySource(vFile)) {
            return false
          }
        }
        true
      } else false
    } else {
      false
    }
  }
}
