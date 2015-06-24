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

package com.imaginea.kodebeagle.action;

import com.imaginea.kodebeagle.model.CodeInfo;
import com.imaginea.kodebeagle.ui.WrapLayout;
import com.imaginea.kodebeagle.util.ESUtils;
import com.imaginea.kodebeagle.util.EditorDocOps;
import com.imaginea.kodebeagle.util.JSONUtils;
import com.imaginea.kodebeagle.ui.MainWindow;
import com.imaginea.kodebeagle.ui.ProjectTree;
import com.imaginea.kodebeagle.util.WindowEditorOps;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.intellij.icons.AllIcons;
import com.intellij.ide.BrowserUtil;
import com.intellij.ide.DataManager;
import com.intellij.ide.util.PropertiesComponent;
import com.intellij.notification.Notification;
import com.intellij.notification.NotificationType;
import com.intellij.notification.Notifications;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.DataConstants;
import com.intellij.openapi.actionSystem.DataContext;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.editor.EditorFactory;
import com.intellij.openapi.fileEditor.FileEditorManager;
import com.intellij.openapi.fileTypes.FileType;
import com.intellij.openapi.fileTypes.FileTypeManager;
import com.intellij.openapi.progress.PerformInBackgroundOption;
import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.ProgressManager;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.ui.JBColor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.JTree;
import javax.swing.ToolTipManager;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import org.jetbrains.annotations.NotNull;

public class RefreshAction extends AnAction {
    private static final String KODE_BEAGLE = "KodeBeagle";
    public static final String EMPTY_ES_URL =
            "<html>Elastic Search URL <br> %s <br> in idea settings is incorrect.<br> See "
                    + "<img src='" + AllIcons.General.Settings + "'/></html>";
    public static final String ES_URL = "esURL";
    public static final String DISTANCE = "distance";
    public static final String SIZE = "size";
    private static final String KODEBEAGLE_SEARCH = "/kodebeagle/_search?source=";
    public static final String ES_URL_DEFAULT = "http://labs.imaginea.com/kodebeagle";
    public static final int DISTANCE_DEFAULT_VALUE = 0;
    public static final int SIZE_DEFAULT_VALUE = 30;
    private static final String EDITOR_ERROR = "Could not get any active editor";
    private static final String FORMAT = "%s %s %s";
    private static final String QUERYING = "Querying";
    private static final String FOR = "for";
    public static final String EXCLUDE_IMPORT_LIST = "Exclude imports";
    public static final String EXCLUDE_IMPORT_LIST_DEFAULT = "";
    public static final String HELP_MESSAGE =
            "<html>Got nothing to search. To begin using, "
                    + "<br /> please select some code and hit <img src='"
                    + AllIcons.Actions.Refresh + "' /> <br/> "
                    + "<br/><b>Please Note:</b> We ignore import statements <br/>"
                    + "while searching - as part of our "
                    + "internal optimization. <br/> <i>So "
                    + "selecting import statements has no effect. </i></html>";
    private static final String QUERY_HELP_MESSAGE =
            "<html><body> <p> <i><b>We tried querying our servers with : </b></i> <br /> %s </p>"
                    + "<i><b>but found no results in response.</i></b>"
                    + "<p> <br/><b>Tip:</b> Try narrowing your selection to fewer lines. "
                    + "<br/>Alternatively, setup \"Exclude imports\" in settings <img src='"
                    + AllIcons.General.Settings + "'/> "
                    + "</p></body></html>";
    private static final String REPO_SCORE = "Score: ";
    private static final String BANNER_FORMAT = "%s %s %s";
    private static final String HTML_U = "<html><u>";
    private static final String U_HTML = "</u></html>";
    private static final String FILETYPE_HELP = "<html><center>Currently KodeBeagle supports "
            + "\"java\" files only.</center></html>";
    private static final String REPO_BANNER_FORMAT = "%s %s";
    private static final String GITHUB_LINK = "https://github.com/";
    private static final String GOTO_GITHUB = "Go to GitHub";
    private static final String FETCHING_PROJECTS = "Fetching projects...";
    private static final String FETCHING_FILE_CONTENTS = "Fetching file contents...";
    private static final String KODEBEAGLE = "Kodebeagle";
    private static final double INDICATOR_FRACTION = 0.5;
    public static final int MAX_EDITORS_DEFAULT_VALUE = 10;
    public static final String MAX_TINY_EDITORS = "maxTinyEditors";
    private static final String PROJECT_ERROR = "Unable to get Project. Please Try again";
    private static final int CHUNK_SIZE = 5;

    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private ProjectTree projectTree = new ProjectTree();
    private EditorDocOps editorDocOps = new EditorDocOps();
    private ESUtils esUtils = new ESUtils();
    private JSONUtils jsonUtils = new JSONUtils();
    private PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
    private JTabbedPane jTabbedPane;
    private List<CodeInfo> codePaneTinyEditorsInfoList = new ArrayList<CodeInfo>();
    private int maxTinyEditors;

    public RefreshAction() {
        super(KODE_BEAGLE, KODE_BEAGLE, AllIcons.Actions.Refresh);
    }

    @Override
    public final void actionPerformed(@NotNull final AnActionEvent anActionEvent) {
        try {
            init();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public final void runAction() throws IOException {
        Project project = windowObjects.getProject();
        final Editor projectEditor = FileEditorManager.getInstance(project).getSelectedTextEditor();

        if (projectEditor != null) {
            windowObjects.getFileNameContentsMap().clear();
            windowObjects.getFileNameNumbersMap().clear();
            final JTree jTree = windowObjects.getjTree();
            final DefaultTreeModel model = (DefaultTreeModel) jTree.getModel();
            final DefaultMutableTreeNode root = (DefaultMutableTreeNode) model.getRoot();
            root.removeAllChildren();
            jTree.setVisible(true);
            windowObjects.getCodePaneTinyEditorsJPanel().removeAll();

            if (editorDocOps.isJavaFile(projectEditor.getDocument())) {
                Set<String> finalImports = getFinalImports(projectEditor.getDocument());
                if (!finalImports.isEmpty()) {
                    Set<String> importsInLines = getImportsInLines(projectEditor, finalImports);
                    if (!importsInLines.isEmpty()) {
                        Notification notification = showQueryTokensNotification(importsInLines);
                        ProgressManager.getInstance().run(new QueryBDServerTask(importsInLines,
                                finalImports, jTree, model, root, notification));
                    } else {
                        showHelpInfo(HELP_MESSAGE);
                    }
                } else {
                    showHelpInfo(HELP_MESSAGE);
                }
            } else {
                showHelpInfo(FILETYPE_HELP);
            }
        } else {
            showHelpInfo(EDITOR_ERROR);
        }
    }

    private void doFrontEndWork(final JTree jTree, final DefaultTreeModel model,
                                final DefaultMutableTreeNode root,
                                final List<CodeInfo> codePaneTinyEditors,
                                final Map<String, ArrayList<CodeInfo>> projectNodes) {
        updateMainPaneJTreeUI(jTree, model, root, projectNodes);
        buildCodePane(codePaneTinyEditors);
    }

    private Map<String, ArrayList<CodeInfo>> doBackEndWork(final Set<String> importsInLines,
                                                           final Set<String> finalImports,
                                                           final ProgressIndicator indicator) {
        indicator.setText(FETCHING_PROJECTS);
        String esResultJson = getESQueryResultJson(importsInLines);
        Map<String, ArrayList<CodeInfo>> projectNodes = new HashMap<String, ArrayList<CodeInfo>>();
        if (!esResultJson.equals(EMPTY_ES_URL)) {
            projectNodes = getProjectNodes(finalImports, esResultJson);
            indicator.setFraction(INDICATOR_FRACTION);
            if (!projectNodes.isEmpty()) {
                indicator.setText(FETCHING_FILE_CONTENTS);
                codePaneTinyEditorsInfoList = getCodePaneTinyEditorsInfoList(projectNodes);
                List<String> fileNamesList =
                        getFileNamesListForTinyEditors(codePaneTinyEditorsInfoList);
                if (fileNamesList != null) {
                    putChunkedFileContentInMap(fileNamesList);
                }
            }
        }
        indicator.setFraction(1.0);
        return projectNodes;
    }

    private List<String> getFileNamesListForTinyEditors(final List<CodeInfo> codePaneTinyEditors) {
        List<String> fileNamesList = new ArrayList<String>();
        for (CodeInfo codePaneTinyEditorInfo : codePaneTinyEditors) {
            fileNamesList.add(codePaneTinyEditorInfo.getFileName());
        }
        return fileNamesList;
    }

    private void putChunkedFileContentInMap(final List<String> fileNamesList) {
        int head = 0;
        int tail = CHUNK_SIZE - 1;
        for (int i = 1; i <= (fileNamesList.size() / CHUNK_SIZE); i++) {
            List<String> subFileNamesList = fileNamesList.subList(head, tail);
            esUtils.putContentsForFileInMap(subFileNamesList);
            head = tail + 1;
            tail += CHUNK_SIZE;
        }
    }

    private void updateMainPaneJTreeUI(final JTree jTree, final DefaultTreeModel model,
                                       final DefaultMutableTreeNode root,
                                       final Map<String, ArrayList<CodeInfo>>  projectNodes) {
        projectTree.updateRoot(root, projectNodes);
        model.reload(root);
        jTree.addTreeSelectionListener(projectTree.getTreeSelectionListener(root));
        ToolTipManager.sharedInstance().registerComponent(jTree);
        jTree.setCellRenderer(projectTree.getJTreeCellRenderer());
        jTree.addMouseListener(projectTree.getMouseListener(root));
        windowObjects.getjTreeScrollPane().setViewportView(jTree);
    }

    private String getESQueryResultJson(final Set<String> importsInLines) {
        String esQueryJson = jsonUtils.getESQueryJson(importsInLines, windowObjects.getSize());
        String esQueryResultJson =
            esUtils.getESResultJson(esQueryJson, windowObjects.getEsURL() + KODEBEAGLE_SEARCH);
        return esQueryResultJson;
    }

    protected final void buildCodePane(final List<CodeInfo> codePaneTinyEditors) {
        JPanel codePaneTinyEditorsJPanel = windowObjects.getCodePaneTinyEditorsJPanel();

        sortCodePaneTinyEditorsInfoList(codePaneTinyEditors);

        for (CodeInfo codePaneTinyEditorInfo : codePaneTinyEditors) {
            String fileName = codePaneTinyEditorInfo.getFileName();
            String fileContents = esUtils.getContentsForFile(fileName);
            List<Integer> lineNumbers = codePaneTinyEditorInfo.getLineNumbers();

            String contentsInLines = editorDocOps.getContentsInLines(fileContents, lineNumbers);
            createCodePaneTinyEditor(codePaneTinyEditorsJPanel, codePaneTinyEditorInfo.toString(),
                                     codePaneTinyEditorInfo.getFileName(), contentsInLines);
        }
    }

    private void createCodePaneTinyEditor(final JPanel codePaneTinyEditorJPanel,
                                          final String displayFileName, final String fileName,
                                          final String contents) {
        Document tinyEditorDoc =
                EditorFactory.getInstance().createDocument(contents);
        tinyEditorDoc.setReadOnly(true);
        Project project = windowObjects.getProject();
        FileType fileType =
                FileTypeManager.getInstance().getFileTypeByExtension(MainWindow.JAVA);

        Editor tinyEditor =
                EditorFactory.getInstance().
                        createEditor(tinyEditorDoc, project, fileType, false);
        windowEditorOps.releaseEditor(project, tinyEditor);

        JPanel expandPanel = new JPanel(new WrapLayout());

        final String projectName = esUtils.getProjectName(fileName);

        int repoId = windowObjects.getRepoNameIdMap().get(projectName);
        String stars;
        if (windowObjects.getRepoStarsMap().containsKey(projectName)) {
            stars = windowObjects.getRepoStarsMap().get(projectName).toString();
        } else {
            String repoStarsJson = jsonUtils.getRepoStarsJSON(repoId);
            stars = esUtils.getRepoStars(repoStarsJson);
            windowObjects.getRepoStarsMap().put(projectName, stars);
        }

        final JLabel expandLabel =
                new JLabel(String.format(BANNER_FORMAT, HTML_U, displayFileName, U_HTML));
        expandLabel.setForeground(JBColor.BLUE);
        expandLabel.addMouseListener(
                new CodePaneTinyEditorExpandLabelMouseListener(displayFileName, fileName));
        expandPanel.add(expandLabel);

        final JLabel projectNameLabel =
                new JLabel(String.format(BANNER_FORMAT, HTML_U, projectName, U_HTML));
        projectNameLabel.setForeground(JBColor.blue);
        projectNameLabel.setToolTipText(GOTO_GITHUB);
        projectNameLabel.addMouseListener(new MouseAdapter() {
            public void mouseClicked(final MouseEvent me) {
                if (!projectName.isEmpty()) {
                    BrowserUtil.browse(GITHUB_LINK + projectName);
                }
            }
        });

        expandPanel.add(projectNameLabel);
        expandPanel.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, expandLabel.getMinimumSize().height));
        expandPanel.setToolTipText(String.format(REPO_BANNER_FORMAT, REPO_SCORE, stars));
        expandPanel.revalidate();
        expandPanel.repaint();

        codePaneTinyEditorJPanel.add(expandPanel);

        codePaneTinyEditorJPanel.add(tinyEditor.getComponent());
        codePaneTinyEditorJPanel.revalidate();
        codePaneTinyEditorJPanel.repaint();
    }

    private List<CodeInfo> getCodePaneTinyEditorsInfoList(final Map<String,
                                                          ArrayList<CodeInfo>> projectNodes) {
        int maxEditors = maxTinyEditors;
        int count = 0;
        List<CodeInfo> codePaneTinyEditors = new ArrayList<CodeInfo>();

        for (Map.Entry<String, ArrayList<CodeInfo>> entry : projectNodes.entrySet()) {
            List<CodeInfo> codeInfoList = entry.getValue();
            for (CodeInfo codeInfo : codeInfoList) {
                if (count++ < maxEditors) {
                    codePaneTinyEditors.add(codeInfo);
                }
            }
        }
        return codePaneTinyEditors;
    }

    public final void showHelpInfo(final String info) {
        JPanel centerInfoPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        centerInfoPanel.add(new JLabel(info));
        goToAllPane();
        windowObjects.getjTreeScrollPane().setViewportView(centerInfoPanel);
    }


    private Map<String, ArrayList<CodeInfo>> getProjectNodes(final Set<String> finalImports,
                                                             final String esResultJson) {
        Map<String, String> fileTokensMap = esUtils.getFileTokens(esResultJson);
        Map<String, ArrayList<CodeInfo>> projectNodes =
                projectTree.updateProjectNodes(finalImports, fileTokensMap);
        return projectNodes;
    }

    private Set<String> getFinalImports(final Document document) {
        Set<String> imports =
                editorDocOps.getImports(document, windowObjects.getProject());

        if (!imports.isEmpty()) {
            if (propertiesComponent.isValueSet(EXCLUDE_IMPORT_LIST)) {
                String excludeImport = propertiesComponent.getValue(EXCLUDE_IMPORT_LIST);
                if (excludeImport != null) {
                    imports = editorDocOps.excludeConfiguredImports(imports, excludeImport);
                }
            }
            Set<String> internalImports =
                    editorDocOps.getInternalImports(windowObjects.getProject());
            Set<String> finalImports =
                    editorDocOps.excludeInternalImports(imports, internalImports);
            return finalImports;
        }
        return imports;
    }

    private Set<String> getImportsInLines(final Editor projectEditor,
                                          final Set<String> externalImports) {
        Set<String> lines = editorDocOps.getLines(projectEditor, windowObjects.getDistance());
        Set<String> importsInLines = editorDocOps.importsInLines(lines, externalImports);
        return importsInLines;
    }

    public final void setJTabbedPane(final JTabbedPane pJTabbedPane) {
        this.jTabbedPane = pJTabbedPane;
    }

    public final void init() throws IOException {
        DataContext dataContext = DataManager.getInstance().getDataContext();
        Project project = (Project) dataContext.getData(DataConstants.PROJECT);
        if (project != null) {
            windowObjects.setProject(project);
            windowObjects.setDistance(propertiesComponent.
                    getOrInitInt(DISTANCE, DISTANCE_DEFAULT_VALUE));
            windowObjects.setSize(propertiesComponent.getOrInitInt(SIZE, SIZE_DEFAULT_VALUE));
            windowObjects.setEsURL(propertiesComponent.getValue(ES_URL, ES_URL_DEFAULT));
            maxTinyEditors =
                    propertiesComponent.getOrInitInt(MAX_TINY_EDITORS, MAX_EDITORS_DEFAULT_VALUE);
            windowEditorOps.writeToDocument("", windowObjects.getWindowEditor().getDocument());
            runAction();
        } else {
            showHelpInfo(PROJECT_ERROR);
            goToAllPane();
        }
    }

    private void sortCodePaneTinyEditorsInfoList(final List<CodeInfo> codePaneTinyEditorsList) {
        Collections.sort(codePaneTinyEditorsList, new Comparator<CodeInfo>() {
            @Override
            public int compare(final CodeInfo o1, final CodeInfo o2) {
                Set<Integer> o1HashSet = new HashSet<Integer>(o1.getLineNumbers());
                Set<Integer> o2HashSet = new HashSet<Integer>(o2.getLineNumbers());
                return o2HashSet.size() - o1HashSet.size();
            }
        });
    }

    private Notification showQueryTokensNotification(final Set<String> importsInLines) {
        final Notification notification = new Notification(KODE_BEAGLE,
                String.format(FORMAT, QUERYING,
                        windowObjects.getEsURL(), FOR),
                importsInLines.toString(),
                NotificationType.INFORMATION);
        Notifications.Bus.notify(notification);
        return notification;
    }

    private class CodePaneTinyEditorExpandLabelMouseListener extends MouseAdapter {
        private final String displayFileName;
        private final String fileName;

        public CodePaneTinyEditorExpandLabelMouseListener(final String pDisplayFileName,
                                                          final String pFileName) {
            this.displayFileName = pDisplayFileName;
            this.fileName = pFileName;
        }

        @Override
        public void mouseClicked(final MouseEvent e) {
            VirtualFile virtualFile = editorDocOps.getVirtualFile(displayFileName,
                    windowObjects.getFileNameContentsMap().get(fileName));
            FileEditorManager.getInstance(windowObjects.getProject()).
                    openFile(virtualFile, true, true);
            Document document =
                    EditorFactory.getInstance().createDocument(windowObjects.
                            getFileNameContentsMap().get(fileName));
            editorDocOps.addHighlighting(windowObjects.
                    getFileNameNumbersMap().get(fileName), document);
            editorDocOps.gotoLine(windowObjects.
                    getFileNameNumbersMap().get(fileName).get(0), document);
        }
    }

    private class QueryBDServerTask extends Task.Backgroundable {
        private final Set<String> importsInLines;
        private final Set<String> finalImports;
        private final JTree jTree;
        private final DefaultTreeModel model;
        private final DefaultMutableTreeNode root;
        private final Notification notification;
        private Map<String, ArrayList<CodeInfo>> projectNodes;
        private volatile boolean isFailed;
        private String httpErrorMsg;

        QueryBDServerTask(final Set<String> pImportsInLines, final Set<String> pFinalImports,
                          final JTree pJTree, final DefaultTreeModel pModel,
                          final DefaultMutableTreeNode pRoot, final Notification pNotification) {
            super(windowObjects.getProject(), KODEBEAGLE, true,
                    PerformInBackgroundOption.ALWAYS_BACKGROUND);
            this.importsInLines = pImportsInLines;
            this.finalImports = pFinalImports;
            this.jTree = pJTree;
            this.model = pModel;
            this.root = pRoot;
            this.notification = pNotification;
        }

        @Override
        public void run(@NotNull final ProgressIndicator indicator) {
            try {
                projectNodes = doBackEndWork(importsInLines, finalImports, indicator);
            } catch (RuntimeException rte) {
                rte.printStackTrace();
                httpErrorMsg = rte.getMessage();
                isFailed = true;
            }
        }

        @Override
        public void onSuccess() {
            if (!isFailed) {
                if (!projectNodes.isEmpty()) {
                    try {
                        doFrontEndWork(jTree, model, root, codePaneTinyEditorsInfoList,
                                       projectNodes);
                        goToFeaturedPane();
                    } catch (RuntimeException rte) {
                        rte.printStackTrace();
                    }
                } else {
                    showHelpInfo(String.format(QUERY_HELP_MESSAGE,
                            importsInLines.toString().replaceAll(",", "<br/>")));
                    jTree.updateUI();
                    notification.expire();
                }
            } else {
                showHelpInfo(httpErrorMsg);
            }
        }
    }

    private void goToFeaturedPane() {
        jTabbedPane.setSelectedIndex(0);
    }

    private void goToAllPane() {
        jTabbedPane.setSelectedIndex(1);
    }
}
