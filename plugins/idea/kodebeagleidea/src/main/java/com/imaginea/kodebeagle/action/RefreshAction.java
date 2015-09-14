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

import com.imaginea.kodebeagle.model.Settings;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.tasks.QueryKBServerTask;
import com.imaginea.kodebeagle.ui.KBNotification;
import com.imaginea.kodebeagle.util.EditorDocOps;
import com.imaginea.kodebeagle.util.UIUtils;
import com.imaginea.kodebeagle.util.WindowEditorOps;
import com.intellij.icons.AllIcons;
import com.intellij.ide.DataManager;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.DataConstants;
import com.intellij.openapi.actionSystem.DataContext;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.fileEditor.FileEditorManager;
import com.intellij.openapi.progress.ProgressManager;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Pair;
import com.intellij.ui.classFilter.ClassFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import org.jetbrains.annotations.NotNull;

public class RefreshAction extends AnAction {
    public static final String EMPTY_ES_URL =
            "<html>Elastic Search URL <br> %s <br> in idea settings is incorrect.<br> See "
                    + "<img src='" + AllIcons.General.Settings + "'/></html>";
    public static final String ES_URL = "esURL";
    public static final String ES_URL_VALUES = "esURL Values";
    public static final String ES_URL_CHECKBOX_VALUE = "Es URL checkbox value";
    public static final String ES_URL_DEFAULT_CHECKBOX_VALUE = "false";
    public static final String LINES_FROM_CURSOR = "lines";
    public static final String SIZE = "size";
    public static final String ES_URL_DEFAULT = "http://labs.imaginea.com/kodebeagle";
    public static final int LINES_FROM_CURSOR_DEFAULT_VALUE = 0;
    public static final int SIZE_DEFAULT_VALUE = 30;
    public static final String EDITOR_ERROR =
            "<html><center>Could not get any active editor</center></html>";
    public static final String EXCLUDE_IMPORT_PATTERN = "Exclude imports pattern";
    public static final String EXCLUDE_IMPORT_CHECKBOX_VALUE = "Exclude imports checkbox value";
    public static final String EXCLUDE_IMPORT_DEFAULT_CHECKBOX_VALUE = "false";
    public static final String EXCLUDE_IMPORT_STATE = "Exclude imports state";
    public static final String OLD_EXCLUDE_IMPORT_LIST = "Exclude imports";
    public static final String NOTIFICATION_CHECKBOX_VALUE = "Notification CheckBox Value";
    public static final String LOGGING_CHECKBOX_VALUE = "Logging CheckBox Value";
    public static final String HELP_MESSAGE_IF_CODE_SELECTED =
            "<html><body> <p>No keywords found in current selection."
                    + "<br /> You may expand your selection,<br/>"
                    + "to include more lines. </p><br/>"
                    + "<p>As an optimization in plugin,<br/> we do not include keywords "
                    + "which refer <br/>to imports internal to the project."
                    + "</p></body></html>";

    public static final String HELP_MESSAGE_NO_SELECTED_CODE =
            "<html><body><center><p>Got nothing to search. To begin, "
                    + "<br /> select some code and hit <img src='"
                    + AllIcons.Actions.Refresh + "' /> <br/></center> ";
    private static final String FILETYPE_HELP = "<html><center>Currently KodeBeagle supports "
            + "\"java\" files only.</center></html>";
    public static final String KODEBEAGLE = "KodeBeagle";
    public static final int TOP_COUNT_DEFAULT_VALUE = 10;
    public static final String TOP_COUNT = "Top count";
    private static final String PROJECT_ERROR = "Unable to get Project. Please Try again";
    public static final String OPT_OUT_CHECKBOX_VALUE = "Opt out checkbox value";
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private EditorDocOps editorDocOps = new EditorDocOps();
    private Settings currentSettings;
    private UIUtils uiUtils = new UIUtils();

    public RefreshAction() {
        super(KODEBEAGLE, KODEBEAGLE, AllIcons.Actions.Refresh);
    }

    @Override
    public final void actionPerformed(@NotNull final AnActionEvent anActionEvent) {
        try {
            init();
        } catch (IOException ioe) {
            KBNotification.getInstance().error(ioe);
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
            windowObjects.getSpotlightPaneTinyEditorsJPanel().removeAll();

            if (editorDocOps.isJavaFile(projectEditor.getDocument())) {
                Pair<Integer, Integer> pair =
                        editorDocOps.getLineOffSets(projectEditor, windowObjects.getDistance());
                Map<String, Set<String>> allImports =
                        getAllImportsAfterExcludes(projectEditor, pair);
                if (!allImports.isEmpty()) {
                    ProgressManager.getInstance().run(new QueryKBServerTask(
                            windowObjects.getProject(), allImports, jTree, model, root));

                } else {
                    if (projectEditor.getSelectionModel().hasSelection()) {
                        uiUtils.showHelpInfo(HELP_MESSAGE_IF_CODE_SELECTED);
                    } else {
                        uiUtils.showHelpInfo(HELP_MESSAGE_NO_SELECTED_CODE);
                    }
                }
            } else {
                uiUtils.showHelpInfo(FILETYPE_HELP);
            }
        } else {
            uiUtils.showHelpInfo(EDITOR_ERROR);
        }
    }

    private Map<String, Set<String>> getAllImportsAfterExcludes(
            final Editor projectEditor, final Pair<Integer, Integer> pair) {
        Map<String, Set<String>> importVsMethods =
                editorDocOps.getImportInLines(projectEditor, pair);
        if (!importVsMethods.isEmpty()) {
            if (currentSettings.getImports().getExcludeImportsCheckBoxValue()) {
                List<ClassFilter> importFilters =
                        currentSettings.getImports().getFilterList();
                Set<String> excludeImports = new HashSet<>();
                for (ClassFilter importFilter : importFilters) {
                    if (importFilter.isEnabled()) {
                        excludeImports.add(importFilter.getPattern());
                    }
                }
               if (!excludeImports.isEmpty()) {
                    importVsMethods =
                            editorDocOps.excludeConfiguredImports(importVsMethods, excludeImports);
                }
            }
            Map<String, Set<String>> finalImports =
                    editorDocOps.excludeInternalImports(importVsMethods);
            return finalImports;
        }
        return importVsMethods;
    }

    public final void init() throws IOException {
        DataContext dataContext = DataManager.getInstance().getDataContext();
        Project project = (Project) dataContext.getData(DataConstants.PROJECT);
        currentSettings = new Settings();
        if (project != null) {
            windowObjects.setProject(project);
            windowObjects.setDistance(currentSettings.getLimits().getLinesFromCursor());
            windowObjects.setSize(currentSettings.getLimits().getResultSize());
            windowObjects.setEsURL(currentSettings.getElasticSearch().getSelectedEsURL());
            windowObjects.setMaxTinyEditors(currentSettings.getLimits().getTopCount());
            windowObjects.retrieveIncludeMethods();
            windowEditorOps.writeToDocument("", windowObjects.getWindowEditor().getDocument());
            runAction();
        } else {
            uiUtils.showHelpInfo(PROJECT_ERROR);
            uiUtils.goToAllPane();
       }
    }
}
