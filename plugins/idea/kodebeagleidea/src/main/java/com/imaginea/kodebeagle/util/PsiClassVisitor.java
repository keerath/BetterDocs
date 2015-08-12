package com.imaginea.kodebeagle.util;

import com.intellij.psi.JavaRecursiveElementVisitor;
import com.intellij.psi.PsiClass;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiExpression;
import com.intellij.psi.PsiField;
import com.intellij.psi.PsiJavaCodeReferenceElement;
import com.intellij.psi.PsiLocalVariable;
import com.intellij.psi.PsiReference;
import com.intellij.psi.PsiReferenceExpression;
import com.intellij.psi.PsiType;
import com.intellij.psi.PsiVariable;
import com.intellij.psi.impl.source.tree.JavaElementType;
import com.intellij.psi.util.ClassUtil;
import com.intellij.psi.util.PsiTypesUtil;
import java.util.HashSet;
import java.util.Set;


public class PsiClassVisitor extends JavaRecursiveElementVisitor {
    private static final char DOT = '.';
    private final int startOffset;
    private final int endOffset;
    private final String onDemandImport;
    private final Set<String> fullyQualifiedImports = new HashSet<>();


    public PsiClassVisitor(int pStartOffset, int pEndOffSet, String pOnDemandImport) {
        super();
        this.startOffset = pStartOffset;
        this.endOffset = pEndOffSet;
        this.onDemandImport = pOnDemandImport;
    }

    public Set<String> getFullyQualifiedImports() {
        return fullyQualifiedImports;
    }

    @Override
    public final void visitReferenceElement(PsiJavaCodeReferenceElement reference) {
        super.visitReferenceElement(reference);
        if (startOffset <= reference.getTextOffset() && reference.getTextOffset() <= endOffset) {
            if (!reference.isQualified()) {
                PsiElement element = reference.resolve();
                if (element instanceof PsiClass) {
                    handlePsiClass((PsiClass) element);
                } else if(element instanceof PsiVariable) {
                    handlePsiVariable((PsiVariable) element);
                }
            }
        }
    }

    public void handlePsiClass(PsiClass psiClass) {
        String qualifiedName =  psiClass.getQualifiedName();
        if (qualifiedName != null) {
            addImportIfPackageNameMatches(qualifiedName);
        }
    }

    public void handlePsiVariable(PsiVariable variable) {
        PsiType psiType = variable.getType();
        addImportIfPackageNameMatches(psiType.getCanonicalText());
    }

    public void addImportIfPackageNameMatches(String qualifiedName) {
        String packageName = ClassUtil.extractPackageName(qualifiedName);
        if(packageName.equals(onDemandImport)) {
            fullyQualifiedImports.add(qualifiedName);
        }
    }
}