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

package com.imaginea.kodebeagle.base.ui;

import com.imaginea.kodebeagle.base.model.CodeInfo;
import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.imaginea.kodebeagle.base.util.ESUtils;
import com.imaginea.kodebeagle.base.util.EditorDocOps;
import com.imaginea.kodebeagle.base.util.WindowEditorOps;
import com.intellij.ide.BrowserUtil;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.editor.EditorFactory;
import com.intellij.openapi.editor.impl.DocumentImpl;
import com.intellij.openapi.fileEditor.FileEditorManager;
import com.intellij.openapi.fileTypes.FileType;
import com.intellij.openapi.fileTypes.FileTypeManager;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.ui.JBColor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseWheelEvent;
import java.awt.event.MouseWheelListener;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class SpotlightPane {
    private static final int HGAP = 20;
    private static final int VGAP = 5;
    private static final String BANNER_FORMAT = "%s %s %s";
    private static final String HTML_U = "<html><u>";
    private static final String END_U_HTML = "</u></html>";
    private static final String GITHUB_LINK = "https://github.com/";
    private static final String OPEN_IN_BROWSER = "Open in a browser";
    private static final String REPO_BANNER_FORMAT = "%s %s";
    private static final String REPO_SCORE = "Score: ";
    public static final int CONTEXT_START = 1;
    public static final int CONTEXT_END = 3;
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private EditorDocOps editorDocOps = new EditorDocOps();
    private ESUtils esUtils = new ESUtils();
    private final WrapLayout wrapLayout = new WrapLayout(FlowLayout.CENTER, HGAP, VGAP);

    public final void buildSpotlightPane(final List<CodeInfo> spotlightPaneTinyEditors) {
        JPanel spotlightPaneTinyEditorsJPanel = windowObjects.getSpotlightPaneTinyEditorsJPanel();

        sortSpotlightPaneTinyEditorsInfoList(spotlightPaneTinyEditors);

        for (CodeInfo spotlightPaneTinyEditorInfo : spotlightPaneTinyEditors) {
            String fileName = spotlightPaneTinyEditorInfo.getAbsoluteFileName();
            String fileContents = esUtils.getContentsForFile(fileName);
            List<Integer> lineNumbers = spotlightPaneTinyEditorInfo.getLineNumbers();
            String contentsInLines = editorDocOps.getContentsInLines(fileContents, lineNumbers);
            createSpotlightPaneTinyEditor(spotlightPaneTinyEditorsJPanel,
                    spotlightPaneTinyEditorInfo.getDisplayFileName(),
                    spotlightPaneTinyEditorInfo.getAbsoluteFileName(), contentsInLines);
        }
    }

    private void createSpotlightPaneTinyEditor(final JPanel spotlightPaneTinyEditorJPanel,
                                               final String displayFileName, final String fileName,
                                               final String contents) {
        Document tinyEditorDoc = new DocumentImpl(contents, true, false);
        tinyEditorDoc.setReadOnly(true);
        FileType fileType =
                FileTypeManager.getInstance().getFileTypeByExtension(MainWindowBase.JAVA);

        Editor tinyEditor =
                EditorFactory.getInstance().
                        createEditor(tinyEditorDoc, windowObjects.getProject(), fileType, false);
        tinyEditor.getContentComponent().addMouseWheelListener(new MouseWheelListener() {
            @Override
            public void mouseWheelMoved(final MouseWheelEvent e) {
                spotlightPaneTinyEditorJPanel.dispatchEvent(e);
            }
        });
        windowEditorOps.releaseEditor(windowObjects.getProject(), tinyEditor);

        JPanel expandPanel = new JPanel(wrapLayout);

        final String projectName = esUtils.getProjectName(fileName);
        final JLabel expandLabel =
                new JLabel(String.format(BANNER_FORMAT, HTML_U, displayFileName, END_U_HTML));
        expandLabel.setForeground(JBColor.BLACK);
        expandLabel.addMouseListener(
                new SpotlightPaneTinyEditorExpandLabelMouseListener(displayFileName,
                        fileName,
                        expandLabel));
        expandPanel.add(expandLabel);

        final JLabel projectNameLabel =
                new JLabel(String.format(BANNER_FORMAT, HTML_U, projectName, END_U_HTML));
        projectNameLabel.setForeground(JBColor.BLACK);
        projectNameLabel.setToolTipText(OPEN_IN_BROWSER);
        projectNameLabel.addMouseListener(new MouseAdapter() {
            public void mouseClicked(final MouseEvent me) {
                if (!projectName.isEmpty()) {
                    BrowserUtil.browse(GITHUB_LINK + projectName);
                }
            }

            public void mouseEntered(final MouseEvent me) {
                projectNameLabel.setForeground(JBColor.BLUE);
                projectNameLabel.updateUI();
            }

            public void mouseExited(final MouseEvent me) {
                projectNameLabel.setForeground(JBColor.BLACK);
                projectNameLabel.updateUI();
            }
        });

        expandPanel.add(projectNameLabel);
        expandPanel.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, expandLabel.getMinimumSize().height));
        expandPanel.setToolTipText(String.format(REPO_BANNER_FORMAT, REPO_SCORE,
                windowObjects.getRepoStarsMap().get(projectName)));
        expandPanel.revalidate();
        expandPanel.repaint();

        spotlightPaneTinyEditorJPanel.add(expandPanel);

        spotlightPaneTinyEditorJPanel.add(tinyEditor.getComponent());
        spotlightPaneTinyEditorJPanel.revalidate();
        spotlightPaneTinyEditorJPanel.repaint();
    }

    private void sortSpotlightPaneTinyEditorsInfoList(final List<CodeInfo>
                                                              spotlighPaneTinyEditorsList) {
        Collections.sort(spotlighPaneTinyEditorsList, new Comparator<CodeInfo>() {
            @Override
            public int compare(final CodeInfo o1, final CodeInfo o2) {
                Set<Integer> o1HashSet = new HashSet<Integer>(o1.getLineNumbers());
                Set<Integer> o2HashSet = new HashSet<Integer>(o2.getLineNumbers());
                return o2HashSet.size() - o1HashSet.size();
            }
        });
    }

    private class SpotlightPaneTinyEditorExpandLabelMouseListener extends MouseAdapter {
        private final String displayFileName;
        private final String fileName;
        private final JLabel expandLabel;

        public SpotlightPaneTinyEditorExpandLabelMouseListener(final String pDisplayFileName,
                                                               final String pFileName,
                                                               final JLabel pExpandLabel) {
            this.displayFileName = pDisplayFileName;
            this.fileName = pFileName;
            this.expandLabel = pExpandLabel;
        }

        @Override
        public void mouseClicked(final MouseEvent e) {
            try {
                VirtualFile virtualFile = editorDocOps.getVirtualFile(fileName, displayFileName,
                        windowObjects.getFileNameContentsMap().get(fileName));
                FileEditorManager.getInstance(windowObjects.getProject()).
                        openFile(virtualFile, true, true);
                Document document = new DocumentImpl(
                        windowObjects.getFileNameContentsMap().get(fileName), true, false);
                editorDocOps.addHighlighting(windowObjects.
                        getFileNameNumbersMap().get(fileName), document);
                editorDocOps.gotoLine(windowObjects.
                        getFileNameNumbersMap().get(fileName).get(0), document);
            } catch (Exception exception) {
                KBNotification.getInstance().error(exception);
                exception.printStackTrace();
            }
        }

        @Override
        public void mouseEntered(final MouseEvent e) {
            expandLabel.setForeground(JBColor.BLUE);
            expandLabel.updateUI();

        }

        @Override
        public void mouseExited(final MouseEvent e) {
            expandLabel.setForeground(JBColor.BLACK);
            expandLabel.updateUI();

        }
    }
}
