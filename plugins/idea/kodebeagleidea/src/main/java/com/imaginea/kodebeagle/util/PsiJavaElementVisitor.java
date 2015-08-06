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

import com.intellij.psi.JavaRecursiveElementVisitor;;
import com.intellij.psi.PsiAssignmentExpression;
import com.intellij.psi.PsiDeclarationStatement;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiExpression;
import com.intellij.psi.PsiField;
import com.intellij.psi.PsiJavaCodeReferenceElement;
import com.intellij.psi.PsiMethodCallExpression;
import com.intellij.psi.PsiNewExpression;
import com.intellij.psi.PsiType;
import com.intellij.psi.impl.source.tree.JavaElementType;
import java.util.HashSet;
import java.util.Set;

public class PsiJavaElementVisitor extends JavaRecursiveElementVisitor {

    Set<String> importsSet;
    public PsiJavaElementVisitor() {
        super();
        importsSet = new HashSet<>();
    }

    @Override
    public void visitElement(PsiElement element) {
        super.visitElement(element);
        if (element.getNode().getElementType().equals(JavaElementType.FIELD)) {
            visitField((PsiField) element);
        } else if (element.getNode().getElementType().equals(JavaElementType.NEW_EXPRESSION)) {
            visitNewExpression((PsiNewExpression) element);
        } else if (element.getNode().getElementType().equals(JavaElementType.METHOD_CALL_EXPRESSION)) {
           visitMethodCallExpression((PsiMethodCallExpression) element);
        } else if (element.getNode().getElementType().equals(JavaElementType.ASSIGNMENT_EXPRESSION)) {
            visitAssignmentExpression((PsiAssignmentExpression) element);
        } else if (element.getNode().getElementType().equals(JavaElementType.DECLARATION_STATEMENT)) {
            visitDeclarationStatement((PsiDeclarationStatement) element);
        }
    }

    @Override
    public void visitField(final PsiField field) {

    }

    @Override
    public void visitNewExpression(final PsiNewExpression newExpression) {
        PsiExpression qualifier = newExpression.getQualifier();
        if (qualifier != null) {
           PsiType qualifierType = qualifier.getType();
            if (qualifierType != null) {
               importsSet.add(qualifierType.getCanonicalText());
            }
        }
    }

    @Override
    public void visitMethodCallExpression(final PsiMethodCallExpression methodCallExpression) {

    }

    @Override
    public void visitAssignmentExpression(final PsiAssignmentExpression assignmentExpression) {
        PsiType lExpressionType = assignmentExpression.getLExpression().getType();
        if (lExpressionType != null) {
            importsSet.add(lExpressionType.getCanonicalText());
        }
        PsiExpression rExpression = assignmentExpression.getRExpression();
        if (rExpression != null) {
            PsiType rExpressionType = rExpression.getType();
            if (rExpressionType != null) {
                importsSet.add(rExpressionType.getCanonicalText());
            }
        }
    }

    @Override
    public void visitDeclarationStatement(final PsiDeclarationStatement declarationStatement) {
        for (PsiElement element : declarationStatement.getDeclaredElements()) {
            if (element != null) {
                if (element.getNode().getElementType().equals(JavaElementType.JAVA_CODE_REFERENCE)) {
                    importsSet.add(((PsiJavaCodeReferenceElement) element).getQualifiedName());
                }
            }
        }
    }
}
