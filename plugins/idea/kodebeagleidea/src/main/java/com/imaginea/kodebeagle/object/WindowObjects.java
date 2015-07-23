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

package com.imaginea.kodebeagle.object;

import com.intellij.notification.Notification;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.project.Project;
import com.intellij.ui.components.JBScrollPane;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.JTree;

@SuppressWarnings("PMD")
public final class WindowObjects {

    private static WindowObjects windowObjects = new WindowObjects();

    private Project project;
    private JTree jTree;
    private Editor windowEditor;
    private int distance;
    private int size;
    private String esURL;
    private Map<String, String> fileNameContentsMap = new HashMap<String, String>();
    private Map<String, List<Integer>> fileNameNumbersMap = new HashMap<String, List<Integer>>();
    private Map<String, String> repoStarsMap = new HashMap<String, String>();
    private Map<String, Integer> repoNameIdMap = new HashMap<String, Integer>();
    private String osInfo;
    private String applicationVersion;
    private String pluginVersion;
    private String beagleId;
    private JPanel codePaneTinyEditorsJPanel;
    private JTabbedPane jTabbedPane;

    public Notification getNotification() {
        return notification;
    }

    public void setNotification(final Notification pNotification) {
        this.notification = pNotification;
    }

    private Notification notification;

    private WindowObjects() {

    }

    public String getPluginVersion() {
        return pluginVersion;
    }

    public String getOsInfo() {
        return osInfo;
    }

    public String getApplicationVersion() {
        return applicationVersion;
    }

    public void setOsInfo(final String posInfo) {

        this.osInfo = posInfo;
    }

    public void setApplicationVersion(final String papplicationVersion) {
        this.applicationVersion = papplicationVersion;
    }

    public void setPluginVersion(final String ppluginVersion) {
        this.pluginVersion = ppluginVersion;
    }

    public void setBeagleId(final String pbeagleId) {
        this.beagleId = pbeagleId;
    }

    public String getBeagleId() {
        return beagleId;
    }

    public JBScrollPane getjTreeScrollPane() {
        return jTreeScrollPane;
    }

    private JBScrollPane jTreeScrollPane;

    public Map<String, Integer> getRepoNameIdMap() {
        return repoNameIdMap;
    }

    public Map<String, String> getRepoStarsMap() {
        return repoStarsMap;
    }

    public static WindowObjects getWindowObjects() {
        return windowObjects;
    }

    public void setFileNameContentsMap(final Map<String, String> pfileContents) {
        this.fileNameContentsMap = pfileContents;
    }

    public Map<String, String> getFileNameContentsMap() {
        return fileNameContentsMap;
    }

    public void setProject(final Project pproject) {
        this.project = pproject;
    }

    public Project getProject() {
        return project;
    }

    public void setTree(final JTree pjTree) {
        this.jTree = pjTree;
    }

    public JTree getjTree() {
        return jTree;
    }

    public void setWindowEditor(final Editor pwindowEditor) {
        this.windowEditor = pwindowEditor;
    }

    public Editor getWindowEditor() {
        return windowEditor;
    }

    public void setDistance(final int pdistance) {
        this.distance = pdistance;
    }

    public int getDistance() {
        return distance;
    }

    public void setSize(final int psize) {
        this.size = psize;
    }

    public int getSize() {
        return size;
    }

    public void setEsURL(final String pesURL) {
        this.esURL = pesURL;
    }

    public String getEsURL() {
        return esURL;
    }

    public static WindowObjects getInstance() {
        return windowObjects;
    }

    public void setPanel(final JPanel pCodePaneTinyEditorsJPanel) {
        this.codePaneTinyEditorsJPanel = pCodePaneTinyEditorsJPanel;
    }

    public JPanel getCodePaneTinyEditorsJPanel() {
        return codePaneTinyEditorsJPanel;
    }

    public void setJTreeScrollPane(final JBScrollPane pJTreeScrollPane) {
        this.jTreeScrollPane = pJTreeScrollPane;
    }

    public Map<String, List<Integer>> getFileNameNumbersMap() {
        return fileNameNumbersMap;
    }

    public void setFileNameNumbersMap(final Map<String, List<Integer>> pFileNameNumbersMap) {
        this.fileNameNumbersMap = pFileNameNumbersMap;
    }

    public JTabbedPane getjTabbedPane() {
        return jTabbedPane;
    }

    public void setjTabbedPane(final JTabbedPane pJTabbedPane) {
        this.jTabbedPane = pJTabbedPane;
    }

}
