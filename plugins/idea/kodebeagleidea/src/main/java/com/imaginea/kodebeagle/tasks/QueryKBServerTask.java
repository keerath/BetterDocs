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

package com.imaginea.kodebeagle.tasks;

import com.google.common.collect.Lists;
import com.imaginea.kodebeagle.model.CodeInfo;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.ui.KBNotification;
import com.imaginea.kodebeagle.ui.ProjectTree;
import com.imaginea.kodebeagle.ui.SpotlightPane;
import com.imaginea.kodebeagle.util.ESUtils;
import com.imaginea.kodebeagle.util.JSONUtils;
import com.imaginea.kodebeagle.util.UIUtils;
import com.intellij.icons.AllIcons;
import com.intellij.notification.Notification;
import com.intellij.notification.NotificationType;
import com.intellij.openapi.progress.PerformInBackgroundOption;
import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.project.Project;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

import javax.swing.JTree;
import javax.swing.ToolTipManager;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryKBServerTask extends Task.Backgroundable {
    public static final String EMPTY_ES_URL =
            "<html>Elastic Search URL <br> %s <br> in idea settings is incorrect.<br> See "
                    + "<img src='" + AllIcons.General.Settings + "'/></html>";
    private static final String FORMAT = "%s %s %s";
    private static final String QUERIED = "Queried";
    private static final String FOR = "for <br/>";
    private static final String QUERY_HELP_MESSAGE =
            "<html><body> <p style= \\\"padding-left:0.15cm;\\\"> "
                    + "<i><b>We tried querying our servers with : </b></i> <br/> %s"
                    + "<i><b><br/>but found no results in response.</i></b></p>";
    private static final String PRO_TIP =
            "<p style= \\\"padding-left:0.15cm;\\\"> <br/>"
                    + "<b>Tip:</b> Try narrowing your selection to fewer lines. "
                    + "<br/>Alternatively, \"Configure imports\" in settings <img src='"
                    + AllIcons.General.Settings + "'/>. "
                    + "</p></body></html>";
    private static final String FETCHING_PROJECTS = "Fetching projects...";
    private static final String FETCHING_FILE_CONTENTS = "Fetching file contents...";
    public static final String KODEBEAGLE = "KodeBeagle";
    private static final double INDICATOR_FRACTION = 0.5;
    private static final int CHUNK_SIZE = 5;
    private static final double CONVERT_TO_SECONDS = 1000000000.0;
    private static final String RESULT_NOTIFICATION_FORMAT =
            "<br/> Showing %d of %d results (%.2f seconds)";
    private static final String KODEBEAGLE_SEARCH = "/importsmethods/_search?source=";
    public static final int MIN_IMPORT_SIZE = 3;
    private final Map<String, Set<String>> finalImports;
    private final JTree jTree;
    private final DefaultTreeModel model;
    private final DefaultMutableTreeNode root;
    private Notification notification;
    private Map<String, ArrayList<CodeInfo>> projectNodes;
    private volatile boolean isFailed;
    private String httpErrorMsg;
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private ProjectTree projectTree = new ProjectTree();
    private ESUtils esUtils = new ESUtils();
    private JSONUtils jsonUtils = new JSONUtils();
    private List<CodeInfo> spotlightPaneTinyEditorsInfoList = new ArrayList<CodeInfo>();
    private UIUtils uiUtils = new UIUtils();

    public QueryKBServerTask(final Project project, final Map<String, Set<String>> pFinalImports,
                             final JTree pJTree, final DefaultTreeModel pModel,
                             final DefaultMutableTreeNode pRoot) {
        super(project, KODEBEAGLE, true,
                PerformInBackgroundOption.ALWAYS_BACKGROUND);
        this.finalImports = pFinalImports;
        this.jTree = pJTree;
        this.model = pModel;
        this.root = pRoot;
    }

    @Override
    public final void run(@NotNull final ProgressIndicator indicator) {
        try {
            long startTime = System.nanoTime();
            doBackEndWork(indicator);
            long endTime = System.nanoTime();
            double timeToFetchResults = (endTime - startTime) / CONVERT_TO_SECONDS;

            String notificationTitle = String.format(FORMAT, QUERIED,
                    windowObjects.getEsURL(), FOR);
            int resultCount = esUtils.getResultCount();
            if (resultCount > 0) {
                String notificationContent = " "
                        + getResultNotificationMessage(resultCount,
                        esUtils.getTotalHitsCount(), timeToFetchResults);
                notification = KBNotification.getInstance()
                        .notifyBalloon(notificationTitle + notificationContent,
                                NotificationType.INFORMATION);
            }
        } catch (RuntimeException rte) {
            KBNotification.getInstance().error(rte);
            rte.printStackTrace();
            httpErrorMsg = rte.getMessage();
            isFailed = true;
        }
    }

    private String getResultNotificationMessage(final int resultCount, final long totalCount,
                                                final double timeToFetchResults) {
        String notificationContent = getNotificationContent();
        return notificationContent + String.format(RESULT_NOTIFICATION_FORMAT,
                resultCount, totalCount, timeToFetchResults);
    }

    private String getNotificationContent() {
        StringBuilder notificationContent = new StringBuilder();
        if (finalImports != null) {
            Set<Map.Entry<String, Set<String>>> entrySet = finalImports.entrySet();
            Iterator<Map.Entry<String, Set<String>>> iterator = entrySet.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Set<String>> next = iterator.next();
                notificationContent.append(next.getKey());
                Set<String> methods = next.getValue();
                if (!methods.isEmpty()) {
                    notificationContent.append(" " + methods.toString());
                }
                if (iterator.hasNext()) {
                    notificationContent.append(",<br/>");
                }
            }
        }
        return notificationContent.toString();
    }

    @Override
    public final void onSuccess() {
        if (!isFailed) {
            if (!projectNodes.isEmpty()) {
                try {
                    doFrontEndWork();
                    uiUtils.goToSpotlightPane();
                } catch (RuntimeException rte) {
                    KBNotification.getInstance().error(rte);
                    rte.printStackTrace();
                }
            } else {
                String helpMsg = String.format(QUERY_HELP_MESSAGE,
                        getNotificationContent());
                if (finalImports.size() > MIN_IMPORT_SIZE) {
                    helpMsg = helpMsg + PRO_TIP;
                }
                uiUtils.showHelpInfo(helpMsg);
                jTree.updateUI();
                if (notification != null) {
                    notification.expire();
                }
            }
        } else {
            uiUtils.showHelpInfo(httpErrorMsg);
        }
    }

    private void doBackEndWork(final ProgressIndicator indicator) {
        indicator.setText(FETCHING_PROJECTS);
        String esResultJson = getESQueryResultJson();
        if (!esResultJson.equals(EMPTY_ES_URL)) {
            projectNodes = getProjectNodes(esResultJson);
            indicator.setFraction(INDICATOR_FRACTION);
            if (!projectNodes.isEmpty()) {
                indicator.setText(FETCHING_FILE_CONTENTS);
                spotlightPaneTinyEditorsInfoList =
                        getSpotlightPaneTinyEditorsInfoList();
                List<String> fileNamesList = getFileNamesListForTinyEditors();
                if (fileNamesList != null) {
                    putChunkedFileContentInMap(fileNamesList);
                }
            }
        }
        indicator.setFraction(1.0);
    }

    private void doFrontEndWork() {
        SpotlightPane spotlightPane = new SpotlightPane();
        updateMainPaneJTreeUI();
        spotlightPane.buildSpotlightPane(spotlightPaneTinyEditorsInfoList);
    }

    private void updateMainPaneJTreeUI() {
        projectTree.updateRoot(root, projectNodes);
        model.reload(root);
        jTree.addTreeSelectionListener(projectTree.getTreeSelectionListener(root));
        ToolTipManager.sharedInstance().registerComponent(jTree);
        jTree.setCellRenderer(projectTree.getJTreeCellRenderer());
        jTree.addMouseListener(projectTree.getMouseListener(root));
        jTree.addKeyListener(projectTree.getKeyListener());
        windowObjects.getjTreeScrollPane().setViewportView(jTree);
    }

    private void putChunkedFileContentInMap(final List<String> fileNamesList) {
        List<List<String>> subLists = Lists.partition(fileNamesList, CHUNK_SIZE);
        for (List<String> subList : subLists) {
            esUtils.fetchContentsAndUpdateMap(subList);
        }
    }

    private List<String> getFileNamesListForTinyEditors() {
        List<String> fileNamesList = new ArrayList<String>();
        for (CodeInfo spotlightPaneTinyEditorInfo : spotlightPaneTinyEditorsInfoList) {
            fileNamesList.add(spotlightPaneTinyEditorInfo.getAbsoluteFileName());
        }
        return fileNamesList;
    }

    private List<CodeInfo> getSpotlightPaneTinyEditorsInfoList() {
        int maxEditors = windowObjects.getMaxTinyEditors();
        int count = 0;
        List<CodeInfo> spotlightPaneTinyEditors = new ArrayList<CodeInfo>();

        for (Map.Entry<String, ArrayList<CodeInfo>> entry : projectNodes.entrySet()) {
            List<CodeInfo> codeInfoList = entry.getValue();
            for (CodeInfo codeInfo : codeInfoList) {
                if (count++ < maxEditors) {
                    spotlightPaneTinyEditors.add(codeInfo);
                }
            }
        }
        return spotlightPaneTinyEditors;
    }

    private Map<String, ArrayList<CodeInfo>> getProjectNodes(final String esResultJson) {
        Map<String, String> fileTokensMap = esUtils.getFileTokens(esResultJson);
        Map<String, ArrayList<CodeInfo>> pProjectNodes =
                projectTree.updateProjectNodes(finalImports.keySet(), fileTokensMap);
        return pProjectNodes;
    }

    private String getESQueryResultJson() {
        String esQueryJson = jsonUtils.getESQueryJson(finalImports, windowObjects.getSize(),
                windowObjects.isIncludeMethods());
        String esQueryResultJson =
                esUtils.getESResultJson(esQueryJson, windowObjects.getEsURL() + KODEBEAGLE_SEARCH);
        return esQueryResultJson;
    }
}
