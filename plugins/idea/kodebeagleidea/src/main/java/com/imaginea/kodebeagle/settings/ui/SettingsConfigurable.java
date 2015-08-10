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

package com.imaginea.kodebeagle.settings.ui;

import com.imaginea.kodebeagle.model.ElasticSearch;
import com.imaginea.kodebeagle.model.Identity;
import com.imaginea.kodebeagle.model.Imports;
import com.imaginea.kodebeagle.model.Limits;
import com.imaginea.kodebeagle.model.Notifications;
import com.imaginea.kodebeagle.model.Settings;
import com.imaginea.kodebeagle.model.SettingsBuilder;
import com.intellij.openapi.options.Configurable;
import com.intellij.openapi.ui.ComboBox;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JTextField;
import org.jetbrains.annotations.Nls;
import org.jetbrains.annotations.Nullable;

public class SettingsConfigurable implements Configurable {
    public static final String KODE_BEAGLE_SETTINGS = "KodeBeagle Settings";
    private SettingsPanel settingsPanel = new SettingsPanel();
    private JLabel beagleIdValue;
    private ComboBox esURLComboBox;
    private IdentityPanel identityPanel;
    private LimitsPanel limitsPanel;
    private ImportsPanel importsPanel;
    private ElasticSearchPanel elasticSearchPanel;
    private NotificationPanel notificationPanel;

    private void getFields() {
        beagleIdValue = settingsPanel.getBeagleIdValue();
        esURLComboBox = settingsPanel.getEsURLComboBox();
        identityPanel = settingsPanel.getIdentityPanel();
        limitsPanel = settingsPanel.getLimitsPanel();
        importsPanel = settingsPanel.getImportsPanel();
        elasticSearchPanel = settingsPanel.getElasticSearchPanel();
        notificationPanel = settingsPanel.getNotificationPanel();
    }

    @Nullable
    @Override
    public final JComponent createComponent() {
        JComponent panel = settingsPanel.createPanel();
        getFields();
        return panel;
    }

    @Override
    public final boolean isModified() {
        boolean isModified = false;
        Settings oldSettings = new Settings();
        Settings newSettings = getNewSettings();

        if (!oldSettings.equals(newSettings)) {
            isModified = true;
        }
        return isModified;
    }

    @Override
    public final void apply() {
        settingsPanel.getImportsPatternFilter().stopEditing();
        Settings newSettings = getNewSettings();
        List<String> currentEsURLs =
                new ArrayList<>(
                        Arrays.asList(newSettings.getElasticSearch().getEsURLS()));
        String esURL = ((JTextField) esURLComboBox.getEditor().getEditorComponent()).getText();
        if (!currentEsURLs.contains(esURL)) {
            currentEsURLs.add(esURL);
            newSettings.getElasticSearch().setEsURLS(
                    currentEsURLs.toArray(new String[currentEsURLs.size()]));
        }
        newSettings.getIdentity().loadBeagleId();
        beagleIdValue.setText(newSettings.getIdentity().getBeagleIdValue());
        newSettings.save();
    }

    private Settings getNewSettings() {
        Identity identity = identityPanel.getIdentity();
        Limits limits = limitsPanel.getLimits();
        Imports imports = importsPanel.getImports();
        ElasticSearch elasticSearch = elasticSearchPanel.getElasticSearch();
        Notifications notifications = notificationPanel.getNotifications();
        return SettingsBuilder.settings().withIdentity(identity)
                .withLimits(limits).withImports(imports)
                .withElasticSearch(elasticSearch)
                .withNotifications(notifications).build();
    }

    @Override
    public final void reset() {
        Settings mySettings = new Settings();
        identityPanel.reset(mySettings.getIdentity());
        limitsPanel.reset(mySettings.getLimits());
        importsPanel.reset(mySettings.getImports());
        elasticSearchPanel.reset(mySettings.getElasticSearch());
        notificationPanel.reset(mySettings.getNotifications());
    }

    @Override
    public final void disposeUIResources() {

    }

    @Nls
    @Override
    public final String getDisplayName() {
        return KODE_BEAGLE_SETTINGS;
    }

    @Nullable
    @Override
    public final String getHelpTopic() {
        return null;
    }
}
