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

package com.imaginea.kodebeagle.ui;

import com.imaginea.kodebeagle.util.WindowEditorOps;
import com.imaginea.kodebeagle.model.CodeInfo;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.util.ESUtils;
import com.imaginea.kodebeagle.util.EditorDocOps;
import com.imaginea.kodebeagle.util.JSONUtils;
import com.intellij.ide.BrowserUtil;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.EditorFactory;
import com.intellij.openapi.fileEditor.FileEditorManager;
import com.intellij.openapi.progress.PerformInBackgroundOption;
import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.ProgressManager;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.vfs.VirtualFile;
import org.jetbrains.annotations.NotNull;

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.swing.AbstractAction;
import javax.swing.ImageIcon;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreeNode;

public class ProjectTree {
    private static final String OPEN_IN_NEW_TAB = "Open in New Tab";
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private ESUtils esUtils = new ESUtils();
    private JSONUtils jsonUtils = new JSONUtils();
    private EditorDocOps editorDocOps = new EditorDocOps();
    private static final String GITHUB_LINK = "https://github.com/";
    private static final String GITHUB_ICON = "icons/github_icon.png";
    private static final String RIGHT_CLICK_MENU_ITEM_TEXT = "Open in Browser";

    protected final URL getIconURL() {
        ClassLoader classLoader = this.getClass().getClassLoader();
        return classLoader.getResource(GITHUB_ICON);
    }

    public final TreeSelectionListener getTreeSelectionListener(final TreeNode root) {
        return new TreeSelectionListener() {
            @Override
            public void valueChanged(final TreeSelectionEvent treeSelectionEvent) {
                DefaultMutableTreeNode selectedNode = (DefaultMutableTreeNode)
                        windowObjects.getjTree().getLastSelectedPathComponent();
                if (selectedNode != null && selectedNode.isLeaf() && root.getChildCount() > 0) {
                    final CodeInfo codeInfo = (CodeInfo) selectedNode.getUserObject();
                    ProgressManager.getInstance().run(new FetchFileContentTask(codeInfo));
                }
            }
        };
    }

    private void updateMainPanePreviewEditor(final List<Integer> lineNumbers,
                                             final String fileContents) {
        final Document mainPanePreviewEditorDocument =
                windowObjects.getWindowEditor().getDocument();
        String contentsInLines =
                editorDocOps.getContentsInLines(fileContents, lineNumbers);
        windowEditorOps.writeToDocument(contentsInLines, mainPanePreviewEditorDocument);
    }

    public final Map<String, ArrayList<CodeInfo>> updateProjectNodes(
            final Collection<String> imports, final Map<String, String> fileTokensMap) {
        Map<String, ArrayList<CodeInfo>> projectNodes = new HashMap<String, ArrayList<CodeInfo>>();
        for (Map.Entry<String, String> entry : fileTokensMap.entrySet()) {
            String fileName = entry.getKey();
            String tokens = entry.getValue();
            List<Integer> lineNumbers;

            if (!windowObjects.getFileNameNumbersMap().containsKey(fileName)) {
                lineNumbers = jsonUtils.getLineNumbers(imports, tokens);
                windowObjects.getFileNameNumbersMap().put(fileName, lineNumbers);
            } else {
                lineNumbers = windowObjects.getFileNameNumbersMap().get(fileName);
            }
            CodeInfo codeInfo = new CodeInfo(fileName, lineNumbers);
            String projectName = esUtils.getProjectName(fileName);

            if (projectNodes.containsKey(projectName)) {
                projectNodes.get(projectName).add(codeInfo);
            } else {
                projectNodes.put(projectName,
                                    new ArrayList<CodeInfo>(Collections.singletonList(codeInfo)));
            }
        }
        return projectNodes;
    }

    public final DefaultMutableTreeNode updateRoot(final DefaultMutableTreeNode root,
                                                   final  Map<String,
                                                           ArrayList<CodeInfo>> projectNodes) {
        for (Map.Entry<String, ArrayList<CodeInfo>> entry : projectNodes.entrySet()) {
            root.add(this.getNodes(entry.getKey(), entry.getValue()));
        }
        return root;
    }

    protected final MutableTreeNode getNodes(final String projectName,
                                             final Iterable<CodeInfo> codeInfoCollection) {
        DefaultMutableTreeNode node = new DefaultMutableTreeNode(projectName);
        Collection<String> fileNameSet = new HashSet<String>();
        for (CodeInfo codeInfo : codeInfoCollection) {
            if (!fileNameSet.contains(codeInfo.getAbsoluteFileName())) {
                node.add(new DefaultMutableTreeNode(codeInfo));
                fileNameSet.add(codeInfo.getAbsoluteFileName());
            }
        }
        return node;
    }

    public final MouseListener getMouseListener(final TreeNode root) {
        return new MouseAdapter() {
            @Override
            public void mouseClicked(final MouseEvent mouseEvent) {
                DefaultMutableTreeNode selectedNode = (DefaultMutableTreeNode)
                        windowObjects.getjTree().getLastSelectedPathComponent();
                String url = "";
                if (mouseEvent.isMetaDown() && selectedNode != null
                        && selectedNode.getParent() != null) {
                    final String gitUrl = getGitUrl(selectedNode, url, root);
                    JPopupMenu menu = new JPopupMenu();

                    if (selectedNode.isLeaf()) {
                        final CodeInfo codeInfo = (CodeInfo) selectedNode.getUserObject();

                        menu.add(new JMenuItem(addOpenInNewTabMenuItem(codeInfo))).
                                setText(OPEN_IN_NEW_TAB);
                    }

                    menu.add(new JMenuItem(new AbstractAction() {
                        @Override
                        public void actionPerformed(final ActionEvent actionEvent) {
                            if (!gitUrl.isEmpty()) {
                                BrowserUtil.browse(GITHUB_LINK + gitUrl);
                            }
                        }
                    })).setText(RIGHT_CLICK_MENU_ITEM_TEXT);

                    menu.show(mouseEvent.getComponent(), mouseEvent.getX(), mouseEvent.getY());
                }
            }
        };
    }

    private String getGitUrl(final DefaultMutableTreeNode selectedNode,
                             final String pUrl,
                             final TreeNode root) {
        String url = pUrl;
        if (!selectedNode.isLeaf()) {
            url = selectedNode.getUserObject().toString(); // getting project name
        } else if (root.getChildCount() > 0) {
            final CodeInfo codeInfo = (CodeInfo) selectedNode.getUserObject();
            url = codeInfo.getAbsoluteFileName();
        }
        return url;
    }

    private AbstractAction addOpenInNewTabMenuItem(final CodeInfo codeInfo) {
        return new AbstractAction() {
            @Override
            public void actionPerformed(final ActionEvent actionEvent) {
                try {
                    VirtualFile virtualFile =
                            editorDocOps.getVirtualFile(codeInfo.getAbsoluteFileName(),
                                    codeInfo.getDisplayFileName(), codeInfo.getContents());
                    FileEditorManager.getInstance(windowObjects.getProject()).
                            openFile(virtualFile, true, true);
                    Document document =
                            EditorFactory.getInstance().
                                    createDocument(codeInfo.getContents());
                    editorDocOps.addHighlighting(codeInfo.getLineNumbers(), document);
                    editorDocOps.gotoLine(codeInfo.getLineNumbers().get(0), document);
                } catch (Exception e) {
                    KBNotification.getInstance().error(e);
                    e.printStackTrace();
                }
            }
        };
    }

    private class FetchFileContentTask extends Task.Backgroundable {

        private static final String KODE_BEAGLE = "KodeBeagle";
        private static final String FETCHING_FILE_CONTENT = "Fetching file content ...";
        private final CodeInfo codeInfo;
        private String fileContents;

        public FetchFileContentTask(final CodeInfo pCodeInfo) {
            super(windowObjects.getProject(),
                    KODE_BEAGLE, true, PerformInBackgroundOption.ALWAYS_BACKGROUND);
            this.codeInfo = pCodeInfo;
        }

        @Override
        public void run(@NotNull final ProgressIndicator indicator) {
            indicator.setText(FETCHING_FILE_CONTENT);
            indicator.setFraction(0.0);
            String fileName = codeInfo.getAbsoluteFileName();
            fileContents = esUtils.getContentsForFile(fileName);

            //Setting contents so that we can use that for Open in New Tab
            codeInfo.setContents(fileContents);
            indicator.setFraction(1.0);
        }

        @Override
        public void onSuccess() {
            updateMainPanePreviewEditor(codeInfo.getLineNumbers(), fileContents);
        }
    }

    public final JTreeCellRenderer getJTreeCellRenderer() {
        return new JTreeCellRenderer();
    }
}

class JTreeCellRenderer implements TreeCellRenderer {
    private static final String REPO_STARS = "Repo Stars: ";
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private ESUtils esUtils = new ESUtils();
    private ProjectTree projectTree = new ProjectTree();
    private DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();

    public final Component getTreeCellRendererComponent(final JTree tree, final Object value,
                                                  final boolean selected, final boolean expanded,
                                                  final boolean leaf, final int row,
                                                  final boolean hasFocus) {
        renderer.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);

            if (value != null && value instanceof DefaultMutableTreeNode) {
                if (!((DefaultMutableTreeNode) value).isLeaf()
                        && !((DefaultMutableTreeNode) value).isRoot()) {
                    String repoName = ((DefaultMutableTreeNode) value).getUserObject().toString();
                    int repoId = windowObjects.getRepoNameIdMap().get(repoName);
                    String stars = esUtils.extractRepoStars(repoName, repoId);
                    renderer.setToolTipText(REPO_STARS + stars);
                    renderer.setIcon(new ImageIcon(projectTree.getIconURL()));
                } else {
                    renderer.setIcon(null);
                    renderer.setToolTipText(null);
                }
            }
        return renderer;
    }
 }
