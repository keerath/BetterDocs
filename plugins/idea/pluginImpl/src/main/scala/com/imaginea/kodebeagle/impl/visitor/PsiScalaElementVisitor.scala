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

package com.imaginea.kodebeagle.impl.visitor

import java.util

import com.imaginea.kodebeagle.base.`object`.WindowObjects
import com.intellij.codeInsight.navigation.actions.GotoTypeDeclarationAction
import com.intellij.openapi.fileEditor.FileEditorManager
import com.intellij.psi.PsiElement
import com.intellij.psi.util.PsiTreeUtil
import org.jetbrains.plugins.scala.lang.parser.ScalaElementTypes
import org.jetbrains.plugins.scala.lang.psi.ScalaPsiElement
import org.jetbrains.plugins.scala.lang.psi.api.ScalaRecursiveElementVisitor
import org.jetbrains.plugins.scala.lang.psi.api.expr.{ScArgumentExprList, ScInfixExpr, ScReferenceExpression}
import org.jetbrains.plugins.scala.lang.psi.api.toplevel.typedef.ScTypeDefinition
import org.jetbrains.plugins.scala.lang.psi.impl.base.types.ScSimpleTypeElementImpl
import org.jetbrains.plugins.scala.lang.psi.types.ScType
import org.jetbrains.plugins.scala.lang.psi.types.result.TypingContext
import org.jetbrains.plugins.scala.lang.refactoring.util.ScTypeUtil

import scala.collection.JavaConverters._

class PsiScalaElementVisitor(start: Int, end: Int) extends ScalaRecursiveElementVisitor {
  val importVsMethods = new util.HashMap[String, util.Set[String]]()
  private val project = WindowObjects.getInstance().getProject
  private val currentEditor = FileEditorManager.getInstance(project).getSelectedTextEditor

  private def addInMap(qualifiedName: String, methodNames: util.Set[String]) {
    if (importVsMethods.containsKey(qualifiedName)) {
      val methods: util.Set[String] = importVsMethods.get(qualifiedName)
      methods.addAll(methodNames)
      importVsMethods.put(qualifiedName, methods)
    }
    else {
      importVsMethods.put(qualifiedName, methodNames)
    }
  }

  override def visitElement(element: ScalaPsiElement): Unit = {
    super.visitElement(element)
    if (inSelectionScope(element)) {
      if (element.getNode.getElementType.equals(ScalaElementTypes.SIMPLE_TYPE)) {
        visitSimpleType(element.asInstanceOf[ScSimpleTypeElementImpl])
      }
    }
  }

  private def visitSimpleType(typeElement: ScSimpleTypeElementImpl): Unit = {
    typeElement.getType(TypingContext.empty).toOption match {
      case Some(x: ScType) =>
        val fqName: String = ScTypeUtil.stripTypeArgs(x).canonicalText.toString
        if (!fqName.contains("Predef")) {
          addInMap(fqName.stripPrefix("_root_."),
            Set[String]().asJava)
        }
      case None =>
    }
  }

  override def visitReferenceExpression(referenceExpression: ScReferenceExpression): Unit = {
    if (inSelectionScope(referenceExpression) &&
      !isParentOfType(referenceExpression, classOf[ScArgumentExprList]) &&
      !isParentOfType(referenceExpression, classOf[ScInfixExpr])) {
      val expr: Option[PsiElement] = getDeepestRefExpr(referenceExpression)
      expr match {
        case Some(x) => val fqName = resolveType(x)
          fqName.nonEmpty match {
            case true => x.getParent match {
              case refExpr: ScReferenceExpression => addInMap(fqName,
                Set(refExpr.getCanonicalText().toString).asJava)
              case _ =>
            }
            case false =>
          }
        case None =>
      }
    }
  }

  private def inSelectionScope(element: ScalaPsiElement): Boolean = {
    start <= element.getTextOffset && element.getTextOffset <= end
  }

  private def isParentOfType[T <: PsiElement](psiElement: PsiElement,
                                              parentClass: Class[T]): Boolean = {
    Option(PsiTreeUtil.getParentOfType(psiElement, parentClass)) match {
      case Some(x) => true
      case None => false
    }
  }

  private def getDeepestRefExpr(referenceExpression: ScReferenceExpression): Option[PsiElement] = {
    referenceExpression.depthFirst match {
      case psiElements: Iterator[PsiElement] => psiElements.filter(x =>
        x.isInstanceOf[ScReferenceExpression] &&
          !isParentOfType(x, classOf[ScArgumentExprList]) &&
          !isParentOfType(x, classOf[ScInfixExpr]) && x.getChildren.isEmpty).toList.headOption
      case _ => None
    }
  }


  private def resolveType(qualifier: PsiElement): String = {
    val typeElement = GotoTypeDeclarationAction.
      findSymbolType(currentEditor, qualifier.getTextOffset)
    typeElement match {
      case typeDef: ScTypeDefinition => typeDef.getTruncedQualifiedName.toString
      case _ => ""
    }
  }
}
