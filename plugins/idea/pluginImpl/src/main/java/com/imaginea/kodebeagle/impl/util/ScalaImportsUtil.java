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

package com.imaginea.kodebeagle.impl.util;

import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.imaginea.kodebeagle.base.util.ImportsUtilBase;
import com.imaginea.kodebeagle.impl.visitor.PsiScalaElementVisitor;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.util.Pair;
import com.intellij.psi.PsiClass;
import com.intellij.psi.PsiDocumentManager;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiMethod;
import com.intellij.psi.util.PsiTreeUtil;
import java.util.Map;
import java.util.Set;
import org.jetbrains.plugins.scala.lang.psi.impl.ScalaFileImpl;

public class ScalaImportsUtil extends ImportsUtilBase {
    private WindowObjects windowObjects = WindowObjects.getInstance();

    @Override
    public final Map<String, Set<String>> getImportInLines(final Editor projectEditor,
                                                           final Pair<Integer, Integer> pair) {
        PsiDocumentManager psiInstance =
                PsiDocumentManager.getInstance(windowObjects.getProject());
        ScalaFileImpl scalaFile = (ScalaFileImpl) psiInstance.getPsiFile(
                projectEditor.getDocument());
        PsiScalaElementVisitor scalaElementVisitor =
                new PsiScalaElementVisitor(pair.getFirst(), pair.getSecond());
        if (scalaFile != null) {
            PsiElement element = scalaFile.findElementAt(pair.getFirst());
            if (element != null) {
                PsiMethod psiMethod = PsiTreeUtil.getParentOfType(element, PsiMethod.class);
                if (psiMethod != null) {
                    psiMethod.accept(scalaElementVisitor);
                } else {
                    PsiClass psiClass = PsiTreeUtil.getParentOfType(element, PsiClass.class);
                    if (psiClass != null) {
                        psiClass.accept(scalaElementVisitor);
                    }
                }
            }
        }
        return scalaElementVisitor.importVsMethods();
    }
}
