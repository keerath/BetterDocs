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

import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.model.Settings;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.intellij.ide.util.PropertiesComponent;
import com.intellij.openapi.options.Configurable;
import com.intellij.openapi.ui.ComboBox;
import com.intellij.ui.classFilter.ClassFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JSlider;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import org.jetbrains.annotations.Nls;
import org.jetbrains.annotations.Nullable;

public class SettingsConfigurable implements Configurable {

    public static final String BEAGLE_ID = "Beagle ID:";
    public static final String KODE_BEAGLE_SETTINGS = "KodeBeagle Settings";
    private static final String NA = "Not Available";
    private SettingsPanel settingsPanel = new SettingsPanel();
    private JLabel beagleIdValue;
    private JSpinner resultSizeSpinner;
    private JSlider linesFromCursorSlider;
    private ComboBox esURLComboBox;
    private JSpinner topCountSpinner;
    private PatternFilterEditor importsPatternFilter;
    private JCheckBox esURLOverrideCheckBox;
    private JCheckBox excludeImportsCheckBox;
    private JCheckBox optOutCheckBox;
    private JCheckBox notificationCheckBox;
    private JCheckBox loggingCheckBox;
    private WindowObjects windowObjects = WindowObjects.getWindowObjects();

    private void getFields() {
        beagleIdValue = settingsPanel.getBeagleIdValue();
        optOutCheckBox = settingsPanel.getOptOutCheckBox();
        resultSizeSpinner = settingsPanel.getResultSizeSpinner();
        linesFromCursorSlider = settingsPanel.getLinesFromCursorSlider();
        esURLComboBox = settingsPanel.getEsURLComboBox();
        topCountSpinner = settingsPanel.getTopCountSpinner();
        importsPatternFilter = settingsPanel.getImportsPatternFilter();
        esURLOverrideCheckBox = settingsPanel.getEsURLOverrideCheckBox();
        excludeImportsCheckBox = settingsPanel.getExcludeImportsCheckBox();
        notificationCheckBox = settingsPanel.getNotificationsCheckBox();
        loggingCheckBox = settingsPanel.getLoggingCheckBox();
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
        Settings newSettings = getSettings();

        if (!oldSettings.equals(newSettings)) {
            isModified = true;
        }
        return isModified;
    }

    @Override
    public final void apply() {
        settingsPanel.getImportsPatternFilter().stopEditing();
        Settings newSettings = getSettings();
        List<String> currentEsURLs =
                new ArrayList<>(
                        Arrays.asList(newSettings.getEsURLComboBoxModel().getEsURLS()));
        String esURL = ((JTextField) esURLComboBox.getEditor().getEditorComponent()).getText();
        if (!currentEsURLs.contains(esURL)) {
            currentEsURLs.add(esURL);
            newSettings.getEsURLComboBoxModel().setEsURLS(
                    currentEsURLs.toArray(new String[currentEsURLs.size()]));
        }
        loadBeagleId(newSettings.getOptOutCheckBoxValue());
        newSettings.save();
    }

    private Settings getSettings() {
        String esURLValue =
                ((JTextField) esURLComboBox
                        .getEditor().getEditorComponent()).getText();
        Boolean esOverrideCheckBoxValue = esURLOverrideCheckBox.isSelected();
        int sizeValue = (int) resultSizeSpinner.getValue();
        int linesFromCursorValue = linesFromCursorSlider.getValue();
        int topCountValue = (int) topCountSpinner.getValue();
        boolean notificationCheckBoxValue = notificationCheckBox.isSelected();
        boolean loggingCheckBoxValue = loggingCheckBox.isSelected();
        List<ClassFilter> filtersList = Arrays.asList(importsPatternFilter.getFilters());
        Boolean excludeImportsCheckBoxValue = excludeImportsCheckBox.isSelected();
        Boolean optOutCheckBoxValue = optOutCheckBox.isSelected();
        return new Settings(new Settings.Limits(linesFromCursorValue, sizeValue, topCountValue),
                new Settings.EsURLComboBoxModel(esURLValue),
                new Settings.Notifications(notificationCheckBoxValue, loggingCheckBoxValue),
                esOverrideCheckBoxValue, filtersList,
                excludeImportsCheckBoxValue, optOutCheckBoxValue);
    }


    private void loadBeagleId(final boolean optOutCheckBoxValue) {
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        if (!optOutCheckBoxValue) {
            if (!propertiesComponent.isValueSet(BEAGLE_ID)) {
                windowObjects.setBeagleId(UUID.randomUUID().toString());
                beagleIdValue.setText(windowObjects.getBeagleId());
            } else {
                beagleIdValue.setText(propertiesComponent.getValue(BEAGLE_ID));
            }
        } else {
            beagleIdValue.setText(NA);
        }
    }

    @Override
    public final void reset() {
        Settings mySettings = new Settings();
        loadBeagleId(mySettings.getOptOutCheckBoxValue());
        optOutCheckBox.setSelected(mySettings.getOptOutCheckBoxValue());
        linesFromCursorSlider.setValue(mySettings.getLimits().getLinesFromCursor());
        resultSizeSpinner.setValue(mySettings.getLimits().getResultSize());
        topCountSpinner.setValue(mySettings.getLimits().getTopCount());
        esURLComboBox.setModel(
                new DefaultComboBoxModel(
                        mySettings.getEsURLComboBoxModel().getEsURLS()));
        notificationCheckBox.setSelected(
                mySettings.getNotifications().getNotificationsCheckBoxValue());
        loggingCheckBox.setSelected(mySettings.getNotifications().getLoggingCheckBoxValue());
        esURLOverrideCheckBox.setSelected(mySettings.getEsOverrideCheckBoxValue());
        List<ClassFilter> filtersList = mySettings.getFilterList();
        excludeImportsCheckBox.setSelected(mySettings.getExcludeImportsCheckBoxValue());
        importsPatternFilter.setFilters(filtersList.toArray(new ClassFilter[filtersList.size()]));
        if (esURLOverrideCheckBox.isSelected()) {
            esURLComboBox.setEnabled(true);
            esURLComboBox.setSelectedItem(mySettings.getEsURLComboBoxModel().getSelectedEsURL());
        } else {
            esURLComboBox.setEnabled(false);
            esURLComboBox.setSelectedItem(RefreshAction.ES_URL_DEFAULT);
        }

        if (excludeImportsCheckBox.isSelected()) {
            importsPatternFilter.setEnabled(true);
        } else {
            importsPatternFilter.setEnabled(false);
        }
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
