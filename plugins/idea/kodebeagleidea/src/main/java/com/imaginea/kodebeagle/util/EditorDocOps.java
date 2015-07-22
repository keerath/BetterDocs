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
import com.intellij.openapi.project.Project;
import com.intellij.openapi.roots.ProjectRootManager;
import com.intellij.openapi.vfs.LocalFileSystem;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.psi.JavaDirectoryService;
import com.intellij.psi.PsiDirectory;
import com.intellij.psi.PsiDocumentManager;
import com.intellij.psi.PsiElement;
import com.intellij.psi.PsiFile;
import com.intellij.psi.PsiImportList;
import com.intellij.psi.PsiImportStatementBase;
import com.intellij.psi.PsiJavaCodeReferenceElement;
import com.intellij.psi.PsiJavaFile;
import com.intellij.psi.PsiManager;
import com.intellij.psi.PsiPackage;
import com.intellij.psi.search.GlobalSearchScope;
import com.intellij.ui.Gray;
import com.intellij.ui.JBColor;
import com.intellij.util.containers.ContainerUtil;
import java.awt.Color;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.jetbrains.annotations.NotNull;

public class EditorDocOps {
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private static final String JAVA_IO_TMP_DIR = "java.io.tmpdir";
    private static final Color HIGHLIGHTING_COLOR =
            new JBColor(new Color(255, 250, 205), Gray._100);
    public static final char DOT = '.';
    private static final String FILE_EXTENSION = "java";

    public final Set<String> importsInLines(final Iterable<String> lines,
                                            final Iterable<String> imports) {
        Set<String> importsInLines = new HashSet<String>();

        for (String line : lines) {
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while (stringTokenizer.hasMoreTokens()) {
                String token = stringTokenizer.nextToken();
                for (String nextImport : imports) {
                    String shortImportName = nextImport.substring(nextImport.lastIndexOf(DOT) + 1);
                    if (token.equalsIgnoreCase(shortImportName)) {
                        importsInLines.add(nextImport);
                    }
                }
            }
        }
        return importsInLines;
    }

    public final Set<String> getLines(final Editor projectEditor, final int distance) {
        Set<String> lines = new HashSet<String>();
        Document document = projectEditor.getDocument();
        String regex = "(\\s*(import|\\/?\\*|//)\\s+.*)|\\\".*";
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

        for (int j = head; j <= tail; j++) {
            String line =
                    document.getCharsSequence().subSequence(document.getLineStartOffset(j),
                            document.getLineEndOffset(j)).toString();
            String cleanedLine = line.replaceFirst(regex, "").replaceAll("\\W+", " ").trim();
            if (!cleanedLine.isEmpty()) {
                lines.add(cleanedLine);
            }
        }
        return lines;
    }

    public final Set<String> getImports(@NotNull final Document document) {
        PsiDocumentManager psiInstance = PsiDocumentManager.getInstance(windowObjects.getProject());
        Set<String> imports = new HashSet<String>();
        if (psiInstance != null && (psiInstance.getPsiFile(document)) != null) {
            PsiFile psiFile = psiInstance.getPsiFile(document);
            if (psiFile != null
                    && psiFile.getFileType().getDefaultExtension().equals(FILE_EXTENSION)) {
                PsiJavaFile psiJavaFile = (PsiJavaFile) psiFile;
                PsiImportList psiImportList = psiJavaFile.getImportList();
                if (psiImportList != null) {
                    PsiImportStatementBase[] importStatements =
                            psiImportList.getAllImportStatements();
                    for (PsiImportStatementBase psiImportStatementBase : importStatements) {
                        if (psiImportStatementBase.getImportReference() != null) {
                            PsiJavaCodeReferenceElement importReference =
                                    psiImportStatementBase.getImportReference();
                            if (importReference != null) {
                                imports.add(importReference.getCanonicalText());
                            }
                        }
                    }
                }
            }
        }
        return imports;
    }

    public final Set<String> getInternalImports(@NotNull final Project project) {
        final Set<String> internalImports = new HashSet<String>();
        GlobalSearchScope scope = GlobalSearchScope.projectScope(project);
        final List<VirtualFile> sourceRoots = new ArrayList<VirtualFile>();
        final ProjectRootManager projectRootManager = ProjectRootManager.getInstance(project);
        ContainerUtil.addAll(sourceRoots, projectRootManager.getContentSourceRoots());
        final PsiManager psiManager = PsiManager.getInstance(project);

        for (final VirtualFile root : sourceRoots) {
            final PsiDirectory directory = psiManager.findDirectory(root);
            if (directory != null) {
                final PsiDirectory[] subdirectories = directory.getSubdirectories();
                for (PsiDirectory subdirectory : subdirectories) {
                    final JavaDirectoryService jDirService =
                            JavaDirectoryService.getInstance();
                    if (jDirService != null && subdirectory != null
                            && jDirService.getPackage(subdirectory) != null) {
                        final PsiPackage rootPackage = jDirService.getPackage(subdirectory);
                        getCompletePackage(rootPackage, scope, internalImports);
                    }
                }
            }
        }
        return internalImports;
    }

    private void getCompletePackage(final PsiPackage completePackage,
                                    final GlobalSearchScope scope,
                                    final Set<String> internalImports) {
        PsiPackage[] subPackages = completePackage.getSubPackages(scope);

        if (subPackages.length != 0) {
            for (PsiPackage psiPackage : subPackages) {
                getCompletePackage(psiPackage, scope, internalImports);
            }
        } else {
            internalImports.add(completePackage.getQualifiedName());
        }
    }

    public final Set<String> excludeInternalImports(final Set<String> imports,
                                                    final Set<String> internalImports) {
        boolean matchFound;
        Set<String> externalImports = new HashSet<String>();
        for (String nextImport : imports) {
            matchFound = false;
            for (String internalImport : internalImports) {
                if (nextImport.startsWith(internalImport)
                        || internalImport.startsWith(nextImport)) {
                    matchFound = true;
                    break;
                }
            }
            if (!matchFound) {
                externalImports.add(nextImport);
            }
        }
        return externalImports;
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
                e.printStackTrace();
            }
        }
        return excludedImports;
    }

    public final VirtualFile getVirtualFile(final String fileName, final String contents) {
        String tempDir = System.getProperty(JAVA_IO_TMP_DIR);
        String filePath = tempDir + "/" + fileName;
        File file = new File(filePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } else {
            VirtualFile virtualFile = LocalFileSystem.getInstance().findFileByPath(filePath);
            windowEditorOps.setWriteStatus(virtualFile, true);
        }
        try {
            BufferedWriter bufferedWriter =
                    new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream(file), ESUtils.UTF_8));
            bufferedWriter.write(contents);
            bufferedWriter.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        file.deleteOnExit();
        VirtualFile virtualFile = LocalFileSystem.getInstance().findFileByPath(filePath);
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
