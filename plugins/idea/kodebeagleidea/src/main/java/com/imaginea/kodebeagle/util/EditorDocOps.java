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

import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.ui.KBNotification;
import com.intellij.codeInsight.highlighting.HighlightUsagesHandler;
import com.intellij.openapi.editor.CaretModel;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.editor.EditorFactory;
import com.intellij.openapi.editor.LogicalPosition;
import com.intellij.openapi.editor.ScrollType;
import com.intellij.openapi.editor.ScrollingModel;
import com.intellij.openapi.editor.SelectionModel;
import com.intellij.openapi.editor.markup.EffectType;
import com.intellij.openapi.editor.markup.HighlighterLayer;
import com.intellij.openapi.editor.markup.HighlighterTargetArea;
import com.intellij.openapi.editor.markup.MarkupModel;
import com.intellij.openapi.editor.markup.TextAttributes;
import com.intellij.openapi.fileEditor.FileEditorManager;
import com.intellij.openapi.roots.PackageIndex;
import com.intellij.openapi.util.Pair;
import com.intellij.openapi.util.io.FileUtil;
import com.intellij.openapi.util.text.StringUtil;
import com.intellij.openapi.vfs.LocalFileSystem;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.psi.PsiClass;
import com.intellij.psi.PsiDocumentManager;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiFile;
import com.intellij.psi.PsiImportList;
import com.intellij.psi.PsiImportStatement;
import com.intellij.psi.PsiJavaFile;
import com.intellij.psi.PsiMethod;
import com.intellij.psi.PsiPackage;
import com.intellij.psi.util.ClassUtil;
import com.intellij.psi.util.PsiTreeUtil;
import com.intellij.ui.Gray;
import com.intellij.ui.JBColor;
import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.jetbrains.annotations.NotNull;

public class EditorDocOps {
    private static final String IMPLICIT_IMPORT = "java.lang";
    private static final String JAVA_IO_TMP_DIR = "java.io.tmpdir";
    private static final String FILE_EXTENSION = "java";
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private static final Color HIGHLIGHTING_COLOR =
            new JBColor(new Color(255, 250, 205), Gray._100);
    public static final char DOT = '.';

    public final Pair<Integer, Integer> getLineOffSets(final Editor projectEditor,
                                                       final int distance) {
        Document document = projectEditor.getDocument();
        SelectionModel selectionModel = projectEditor.getSelectionModel();
        int head = 0;
        int tail = document.getLineCount() - 1;
        if (selectionModel.hasSelection()) {
            head = document.getLineNumber(selectionModel.getSelectionStart());
            tail = document.getLineNumber(selectionModel.getSelectionEnd());
            /*Selection model gives one more line if line is selected completely.
              By Checking if complete line is slected and decreasing tail*/
            if ((document.getLineStartOffset(tail) == selectionModel.getSelectionEnd())) {
                tail--;
            }

        } else {
            int currentLine = document.getLineNumber(projectEditor.getCaretModel().getOffset());

            if (currentLine - distance >= 0) {
                head = currentLine - distance;
            }

            if (currentLine + distance <= document.getLineCount() - 1) {
                tail = currentLine + distance;
            }
        }
        int start = document.getLineStartOffset(head);
        int end = document.getLineEndOffset(tail);
        Pair<Integer, Integer> pair = new Pair<>(start, end);
        return pair;
    }

    public final Set<String> getImportInLines(final Editor projectEditor,
                                              final Pair<Integer, Integer> pair) {
        PsiDocumentManager psiInstance =
                PsiDocumentManager.getInstance(windowObjects.getProject());
        PsiJavaFile psiJavaFile =
                (PsiJavaFile) psiInstance.getPsiFile(projectEditor.getDocument());
        PsiJavaElementVisitor psiJavaElementVisitor =
                new PsiJavaElementVisitor(pair.getFirst(), pair.getSecond());
        Set<String> finalImports = new HashSet<>();
        if (psiJavaFile != null && psiJavaFile.findElementAt(pair.getFirst()) != null) {
            PsiElement psiElement = psiJavaFile.findElementAt(pair.getFirst());
            final PsiElement psiMethod =  PsiTreeUtil.getParentOfType(psiElement, PsiMethod.class);
            if (psiMethod != null) {
                psiMethod.accept(psiJavaElementVisitor);
            } else {
                final PsiClass psiClass = PsiTreeUtil.getParentOfType(psiElement, PsiClass.class);
                if (psiClass != null) {
                    psiClass.accept(psiJavaElementVisitor);
                }
            }
            Set<String> importsInLines = psiJavaElementVisitor.getImportsSet();
            finalImports = getImportsAfterValidation(psiJavaFile, importsInLines);
        }
        return removeImplicitImports(finalImports);
    }

    private Set<String> getImportsAfterValidation(final PsiJavaFile javaFile,
                                                  final Set<String> importInLines) {
        Set<String> finalImports = getFullyQualifiedImports(javaFile, importInLines);
        importInLines.removeAll(finalImports);
        Set<PsiPackage> importedPackages = getOnDemandImports(javaFile);
        if (!importedPackages.isEmpty()) {
            for (PsiPackage psiPackage : importedPackages) {
                for (String psiImport : importInLines) {
                    if (psiPackage.containsClassNamed(ClassUtil.extractClassName(psiImport))) {
                        finalImports.add(psiImport);
                    }
                }
            }
        }
        return finalImports;
    }

    private Set<String> getFullyQualifiedImports(final PsiJavaFile javaFile,
                                                final Set<String> importsInLines) {
        Set<String> fullyQualifiedImports = new HashSet<>();
        PsiImportList importList = javaFile.getImportList();
        Collection<PsiImportStatement> importStatements =
                PsiTreeUtil.findChildrenOfType(importList, PsiImportStatement.class);
        for (PsiImportStatement importStatement : importStatements) {
            if (!importStatement.isOnDemand()) {
                String qualifiedName = importStatement.getQualifiedName();
                if (importsInLines.contains(qualifiedName)) {
                    fullyQualifiedImports.add(qualifiedName);
                }
            }
        }
        return fullyQualifiedImports;
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


    private Set<String> removeImplicitImports(final Set<String> importsInLines) {
        Set<String> excludeImplicitImports = new HashSet<>();
        for (String importValue : importsInLines) {
            if (importValue != null && importValue.startsWith(IMPLICIT_IMPORT)) {
                excludeImplicitImports.add(importValue);
            }
        }
        importsInLines.removeAll(excludeImplicitImports);
        return importsInLines;
    }

    public final Set<String> excludeInternalImports(@NotNull final Set<String> imports) {
        final Set<String> importsAfterExclusion = new HashSet<String>();
        PackageIndex packageIndex = PackageIndex.getInstance(windowObjects.getProject());
        for (String importName : imports) {
            int indexOfDot = importName.lastIndexOf(DOT);
            String packageName;
            if (indexOfDot != -1) {
                packageName = importName.substring(0, importName.lastIndexOf(DOT));
                List<VirtualFile> packageDirectories = Arrays.asList(
                        packageIndex.getDirectoriesByPackageName(packageName, false));
                if (packageDirectories.size() > 0) {
                    VirtualFile packageDirectory = packageDirectories.get(0);
                    if (!packageDirectory.isInLocalFileSystem()) {
                        importsAfterExclusion.add(importName);
                    }
                }
            }
        }
        return importsAfterExclusion;
    }

    public final Set<String> excludeConfiguredImports(final Set<String> imports,
                                                      final Set<String> excludeImports) {
        Set<String> excludedImports = new HashSet<String>();
        imports.removeAll(excludeImports);
        excludedImports.addAll(imports);
        for (String importStatement : excludeImports) {
            try {
                Pattern pattern = Pattern.compile(importStatement);
                for (String nextImport : imports) {
                    Matcher matcher = pattern.matcher(nextImport);
                    if (matcher.find()) {
                        excludedImports.remove(nextImport);
                    }
                }
            } catch (PatternSyntaxException e) {
                KBNotification.getInstance().error(e);
                e.printStackTrace();
            }
        }
        return excludedImports;
    }

    public final VirtualFile getVirtualFile(final String fileName,
                                            final String displayFileName,
                                            final String contents)
            throws IOException, NoSuchAlgorithmException {

        final String tempDir = System.getProperty(JAVA_IO_TMP_DIR);
        final String trimmedFileName =
                FileUtil.sanitizeFileName(StringUtil.trimEnd(fileName, displayFileName));
        final String digest = Utils.getInstance().getDigestAsString(trimmedFileName);
        final String fullFilePath = Utils.getInstance()
                .createFileWithContents(displayFileName, contents, tempDir, digest);
        // Refreshing File System is required so that it is aware of newly created files
        final VirtualFile virtualFile = LocalFileSystem.getInstance()
                .refreshAndFindFileByIoFile(new File(fullFilePath));
        if (virtualFile == null) {
            throw new IllegalArgumentException("Virtual file should not be null."
                    + " Can be an issue with FileSystem.");
        }
        windowEditorOps.setWriteStatus(virtualFile, false);
        return virtualFile;
    }

    public final void addHighlighting(final List<Integer> linesForHighlighting,
                                      final Document document) {
        TextAttributes attributes = new TextAttributes();
        JBColor color = JBColor.GREEN;
        attributes.setEffectColor(color);
        attributes.setEffectType(EffectType.SEARCH_MATCH);
        attributes.setBackgroundColor(HIGHLIGHTING_COLOR);

        Editor projectEditor =
                FileEditorManager.getInstance(windowObjects.getProject()).getSelectedTextEditor();
        if (projectEditor != null) {
            PsiFile psiFile =
                    PsiDocumentManager.getInstance(windowObjects.getProject()).
                            getPsiFile(projectEditor.getDocument());
            MarkupModel markupModel = projectEditor.getMarkupModel();
            if (markupModel != null) {
                markupModel.removeAllHighlighters();

                for (int line : linesForHighlighting) {
                    line = line - 1;
                    if (line < document.getLineCount()) {
                        int startOffset = document.getLineStartOffset(line);
                        int endOffset = document.getLineEndOffset(line);
                        String lineText =
                                document.getCharsSequence().
                                        subSequence(startOffset, endOffset).toString();
                        int lineStartOffset =
                                startOffset + lineText.length() - lineText.trim().length();
                        markupModel.addRangeHighlighter(lineStartOffset, endOffset,
                                HighlighterLayer.ERROR, attributes,
                                HighlighterTargetArea.EXACT_RANGE);
                        if (psiFile != null && psiFile.findElementAt(lineStartOffset) != null) {
                            HighlightUsagesHandler.doHighlightElements(projectEditor,
                                    new PsiElement[]{psiFile.findElementAt(lineStartOffset)},
                                    attributes, false);
                        }
                    }
                }
            }
        }
    }

    public final void gotoLine(final int pLineNumber, final Document document) {
        int lineNumber = pLineNumber;
        Editor projectEditor =
                FileEditorManager.getInstance(windowObjects.getProject()).getSelectedTextEditor();

        if (projectEditor != null) {
            CaretModel caretModel = projectEditor.getCaretModel();

            //document is 0-indexed
            if (lineNumber > document.getLineCount()) {
                lineNumber = document.getLineCount() - 1;
            } else {
                lineNumber = lineNumber - 1;
            }

            caretModel.moveToLogicalPosition(new LogicalPosition(lineNumber, 0));

            ScrollingModel scrollingModel = projectEditor.getScrollingModel();
            scrollingModel.scrollToCaret(ScrollType.CENTER);
        }
    }

    public final String getContentsInLines(final String fileContents,
                                           final List<Integer> lineNumbersList) {
        Document document = EditorFactory.getInstance().createDocument(fileContents);
        Set<Integer> lineNumbersSet = new TreeSet<Integer>(lineNumbersList);

        StringBuilder stringBuilder = new StringBuilder();
        int prev = lineNumbersSet.iterator().next();

        for (int line : lineNumbersSet) {
            //Document is 0 indexed
            line = line - 1;
            if (line < document.getLineCount() - 1) {
                if (prev != line - 1) {
                    stringBuilder.append(System.lineSeparator());
                    prev = line;
                }
                int startOffset = document.getLineStartOffset(line);
                int endOffset = document.getLineEndOffset(line)
                        + document.getLineSeparatorLength(line);
                String code = document.getCharsSequence().
                        subSequence(startOffset, endOffset).
                        toString().trim()
                        + System.lineSeparator();
                stringBuilder.append(code);
            }
        }
        return stringBuilder.toString();
    }

    public final boolean isJavaFile(final Document document) {
        PsiDocumentManager psiInstance =
                PsiDocumentManager.getInstance(windowObjects.getProject());
        if (psiInstance != null && (psiInstance.getPsiFile(document)) != null) {
            PsiFile psiFile = psiInstance.getPsiFile(document);
            if (psiFile != null
                    && psiFile.getFileType().getDefaultExtension().equals(FILE_EXTENSION)) {
                return true;
            }
        }
        return false;
    }
}
