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
import com.imaginea.kodebeagle.impl.visitor.PsiJavaElementVisitor;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.util.Pair;
import com.intellij.psi.PsiClass;
import com.intellij.psi.PsiDocumentManager;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiImportList;
import com.intellij.psi.PsiImportStatement;
import com.intellij.psi.PsiJavaFile;
import com.intellij.psi.PsiMethod;
import com.intellij.psi.PsiPackage;
import com.intellij.psi.util.ClassUtil;
import com.intellij.psi.util.PsiTreeUtil;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JavaImportsUtil extends ImportsUtilBase {

    private static final String IMPLICIT_IMPORT = "java.lang";
    private WindowObjects windowObjects = WindowObjects.getInstance();

    public final Map<String, Set<String>> getImportInLines(final Editor projectEditor,
                                                           final Pair<Integer, Integer> pair) {
        PsiDocumentManager psiInstance =
                PsiDocumentManager.getInstance(windowObjects.getProject());
        PsiJavaFile psiJavaFile =
                (PsiJavaFile) psiInstance.getPsiFile(projectEditor.getDocument());
        PsiJavaElementVisitor psiJavaElementVisitor =
                new PsiJavaElementVisitor(pair.getFirst(), pair.getSecond());
        Map<String, Set<String>> finalImports = new HashMap<>();
        if (psiJavaFile != null && psiJavaFile.findElementAt(pair.getFirst()) != null) {
            PsiElement psiElement = psiJavaFile.findElementAt(pair.getFirst());
            final PsiElement psiMethod = PsiTreeUtil.getParentOfType(psiElement, PsiMethod.class);
            if (psiMethod != null) {
                psiMethod.accept(psiJavaElementVisitor);
            } else {
                final PsiClass psiClass = PsiTreeUtil.getParentOfType(psiElement, PsiClass.class);
                if (psiClass != null) {
                    psiClass.accept(psiJavaElementVisitor);
                }
            }
            Map<String, Set<String>> importVsMethods = psiJavaElementVisitor.getImportVsMethods();
            finalImports = getImportsAndMethodsAfterValidation(psiJavaFile, importVsMethods);
        }
        return removeImplicitImports(finalImports);
    }

    private Map<String, Set<String>> getImportsAndMethodsAfterValidation(
            final PsiJavaFile javaFile, final Map<String, Set<String>> importsVsMethods) {
        Map<String, Set<String>> finalImportsWithMethods =
                getFullyQualifiedImportsWithMethods(javaFile, importsVsMethods);
        Set<String> imports = importsVsMethods.keySet();
        Set<PsiPackage> importedPackages = getOnDemandImports(javaFile);
        if (!importedPackages.isEmpty()) {
            for (PsiPackage psiPackage : importedPackages) {
                for (String psiImport : imports) {
                    if (psiPackage.containsClassNamed(ClassUtil.extractClassName(psiImport))) {
                        finalImportsWithMethods.put(psiImport, importsVsMethods.get(psiImport));
                    }
                }
            }
        }
        return finalImportsWithMethods;
    }

    private Map<String, Set<String>> getFullyQualifiedImportsWithMethods(
            final PsiJavaFile javaFile, final Map<String, Set<String>> importVsMethods) {
        Map<String, Set<String>> fullyQualifiedImportsWithMethods = new HashMap<>();
        PsiImportList importList = javaFile.getImportList();
        Collection<PsiImportStatement> importStatements =
                PsiTreeUtil.findChildrenOfType(importList, PsiImportStatement.class);
        for (PsiImportStatement importStatement : importStatements) {
            if (!importStatement.isOnDemand()) {
                String qualifiedName = importStatement.getQualifiedName();
                if (importVsMethods.containsKey(qualifiedName)) {
                    fullyQualifiedImportsWithMethods.put(qualifiedName,
                            importVsMethods.get(qualifiedName));
                }
            }
        }
        return fullyQualifiedImportsWithMethods;
    }

    private Set<PsiPackage> getOnDemandImports(final PsiJavaFile javaFile) {
        Set<PsiPackage> psiPackages = new HashSet<>();
        PsiElement[] packageImports = javaFile.getOnDemandImports(false, false);
        for (PsiElement packageImport : packageImports) {
            if (packageImport instanceof PsiPackage) {
                psiPackages.add((PsiPackage) packageImport);
            }
        }
        return psiPackages;
    }

    private Map<String, Set<String>> removeImplicitImports(
            final Map<String, Set<String>> importsVsMethods) {
        Map<String, Set<String>> finalImportsVsMethods = new HashMap<>();
        finalImportsVsMethods.putAll(importsVsMethods);
        for (Map.Entry<String, Set<String>> entry : importsVsMethods.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(IMPLICIT_IMPORT)) {
                finalImportsVsMethods.remove(key);
            }
        }
        return finalImportsVsMethods;
    }
}
