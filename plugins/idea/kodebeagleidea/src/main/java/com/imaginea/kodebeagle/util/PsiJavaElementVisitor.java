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

package com.imaginea.kodebeagle.util;

import com.intellij.psi.JavaRecursiveElementVisitor;
import com.intellij.psi.PsiAssignmentExpression;
import com.intellij.psi.PsiDeclarationStatement;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiExpression;
import com.intellij.psi.PsiField;
import com.intellij.psi.PsiMethodCallExpression;
import com.intellij.psi.PsiNewExpression;
import com.intellij.psi.PsiReference;
import com.intellij.psi.PsiReferenceExpression;
import com.intellij.psi.PsiType;
import com.intellij.psi.PsiTypeElement;
import com.intellij.psi.impl.source.tree.JavaElementType;
import com.intellij.psi.util.PsiTreeUtil;
import com.siyeh.ig.psiutils.ClassUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class PsiJavaElementVisitor extends JavaRecursiveElementVisitor {
    private Set<String> importsSet;
    private int startOffset;
    private int endOffset;

    public final Set<String> getImportsSet() {
        return importsSet;
    }

    public PsiJavaElementVisitor(final int start, final int end) {
        super();
        importsSet = new HashSet<>();
        startOffset = start;
        endOffset = end;
    }

    @Override
    public final void visitElement(final PsiElement element) {
        super.visitElement(element);
        if (startOffset <= element.getTextOffset() && element.getTextOffset() <= endOffset) {
            if (element.getNode().getElementType().equals(JavaElementType.FIELD)) {
                visitPsiFields((PsiField) element);
            } else if (element.getNode().getElementType().equals(JavaElementType.NEW_EXPRESSION)) {
                visitPsiNewExpression((PsiNewExpression) element);
            } else if (element.getNode().getElementType().
                    equals(JavaElementType.METHOD_CALL_EXPRESSION)) {
                visitPsiMethodCallExpression((PsiMethodCallExpression) element);
            } else if (element.getNode().getElementType().
                    equals(JavaElementType.ASSIGNMENT_EXPRESSION)) {
                visitPsiAssignmentExpression((PsiAssignmentExpression) element);
            } else if (element.getNode().getElementType().
                    equals(JavaElementType.DECLARATION_STATEMENT)) {
                visitPsiDeclarationStatement((PsiDeclarationStatement) element);
            }
        }
    }

    public final void visitPsiAssignmentExpression(final PsiAssignmentExpression
                                                           assignmentExpression) {
        PsiType lExpressionType = assignmentExpression.getLExpression().getType();
        if (lExpressionType != null && !ClassUtils.isPrimitive(lExpressionType)) {
            String type = removeSpecialSymbols(lExpressionType.getCanonicalText());
            importsSet.add(type);
        }
        PsiExpression rExpression = assignmentExpression.getRExpression();
        if (rExpression != null) {
            PsiType rExpressionType = rExpression.getType();
            if (rExpressionType != null && !ClassUtils.isPrimitive(rExpressionType)) {
                String type = removeSpecialSymbols(rExpressionType.getCanonicalText());
                importsSet.add(type);
            }
        }
    }

    public final void visitPsiDeclarationStatement(final PsiDeclarationStatement
                                                           declarationStatement) {
         Collection<PsiTypeElement> typeElements =
                 PsiTreeUtil.findChildrenOfType(declarationStatement, PsiTypeElement.class);
        for (PsiTypeElement element : typeElements) {
            String type = removeSpecialSymbols(element.getType().getCanonicalText());
            importsSet.add(type);
        }
    }

    public final void visitPsiNewExpression(final PsiNewExpression element) {
        if (element.getType() != null) {
            PsiType psiType = element.getType();
            if (psiType != null && !ClassUtils.isPrimitive(psiType)) {
                String type = removeSpecialSymbols(psiType.getCanonicalText());
                importsSet.add(type);
            }
        }
    }

    public final void visitPsiFields(final PsiField psiField) {
        if (!ClassUtils.isPrimitive(psiField.getType())) {
            String type = removeSpecialSymbols(psiField.getType().getCanonicalText());
            if (psiField.getInitializer() != null) {
                PsiExpression psiExpression = psiField.getInitializer();
                if (psiExpression != null) {
                    PsiType psiType = psiExpression.getType();
                    if (psiType != null && !ClassUtils.isPrimitive(psiType)) {
                        String psiFieldInitializer =
                                removeSpecialSymbols(psiType.getCanonicalText());
                        importsSet.add(psiFieldInitializer);
                    }
                }
            }
            importsSet.add(type);
        }
    }

    public final void visitPsiMethodCallExpression(final PsiMethodCallExpression element) {
        PsiReferenceExpression methodExpr = element.getMethodExpression();
        if (methodExpr.getQualifierExpression() != null) {
            PsiExpression psiExpression = methodExpr.getQualifierExpression();
            if (psiExpression != null) {
                PsiType psiType = psiExpression.getType();
                if (psiType != null && !ClassUtils.isPrimitive(psiExpression.getType())) {
                    String type = removeSpecialSymbols(psiType.getCanonicalText());
                    importsSet.add(type);
                } else if (psiExpression.getReference() != null
                        && !ClassUtils.isPrimitive(psiExpression.getType())) {
                    PsiReference psiReference = psiExpression.getReference();
                    if (psiReference != null) {
                        String type = removeSpecialSymbols(psiReference.getCanonicalText());
                        importsSet.add(type);
                    }
                }
            }
        }
    }

    private String removeSpecialSymbols(final String pType) {
        String type = pType;
        if (type != null && type.contains("<")) {
            type = type.substring(0, type.indexOf("<"));
        } else if (type != null && type.contains("[")) {
            type = type.substring(0, type.indexOf("["));
        }
        return type;
    }
}
