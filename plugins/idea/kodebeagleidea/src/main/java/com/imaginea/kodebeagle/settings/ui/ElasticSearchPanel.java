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
import com.imaginea.kodebeagle.model.ElasticSearch;
import com.intellij.openapi.ui.ComboBox;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;

public class ElasticSearchPanel {

    private static final String TITLE4 = "Elastic Search Server";
    private static final String ELASTIC_SEARCH_URL = "Elastic Search URL:";
    private static final String OVERRIDE = "Override";

    private static final int[] ELASTIC_SEARCH_PANEL_COLUMN_WIDTHS =
            new int[] {29, 84, 63, 285, 0, 95, 0};

    private static final int[] ELASTIC_SEARCH_PANEL_ROW_HEIGHTS = new int[] {0, 0, 0};

    private static final double[] ELASTIC_SEARCH_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private static final double[] ELASTIC_SEARCH_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 1.0E-4};

    private final GridBagConstraints elasticSearchPanelVerticalSpacer1 =
            new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints elasticSearchPanelFirstLeft =
            new GridBagConstraints(1, 1, 5, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0);

    private final GridBagConstraints elasticSearchPanelFirstCenter =
            new GridBagConstraints(3, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 5), 0, 0);

    private final GridBagConstraints elasticSearchPanelFirstRight =
            new GridBagConstraints(5, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0);

    private final JPanel spacer = new JPanel(null);
    private ComboBox esURLComboBox;
    private JCheckBox esURLOverrideCheckBox;

    protected ElasticSearchPanel() {
        createFields();
    }

    public final ComboBox getEsURLComboBox() {
        return esURLComboBox;
    }

    public final JCheckBox getEsURLOverrideCheckBox() {
        return esURLOverrideCheckBox;
    }

    private void createFields() {
        esURLComboBox = new ComboBox();
        esURLComboBox.setEditable(true);
        esURLComboBox.setVisible(true);
        esURLOverrideCheckBox = new JCheckBox();
        esURLOverrideCheckBox.setVisible(true);
        esURLOverrideCheckBox.setText(OVERRIDE);
        esURLOverrideCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                if (esURLComboBox.isEnabled()) {
                    esURLComboBox.setEnabled(false);
                    esURLComboBox.setSelectedItem(RefreshAction.ES_URL_DEFAULT);
                } else {
                    esURLComboBox.setEnabled(true);
                    esURLComboBox.requestFocus();
                }
            }
        });
    }

    public final JPanel getPanel() {
        JPanel elasticSearchPanel = new JPanel();
        elasticSearchPanel.setBorder(new TitledBorder(TITLE4));
        elasticSearchPanel.setLayout(new GridBagLayout());
        ((GridBagLayout) elasticSearchPanel.getLayout()).columnWidths =
                ELASTIC_SEARCH_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) elasticSearchPanel.getLayout()).rowHeights =
                ELASTIC_SEARCH_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) elasticSearchPanel.getLayout()).columnWeights =
                ELASTIC_SEARCH_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) elasticSearchPanel.getLayout()).rowWeights =
                ELASTIC_SEARCH_PANEL_ROW_WEIGHTS;
        elasticSearchPanel.add(spacer, elasticSearchPanelVerticalSpacer1);
        elasticSearchPanel.add(new JLabel(ELASTIC_SEARCH_URL), elasticSearchPanelFirstLeft);
        elasticSearchPanel.add(esURLComboBox, elasticSearchPanelFirstCenter);
        elasticSearchPanel.add(esURLOverrideCheckBox, elasticSearchPanelFirstRight);
        return elasticSearchPanel;
    }

    public final void reset(final ElasticSearch elasticSearch) {
        esURLComboBox.setModel(
                new DefaultComboBoxModel(
                        elasticSearch.getEsURLS()));
        esURLOverrideCheckBox.setSelected(
                elasticSearch.getEsOverrideCheckBoxValue());
        if (elasticSearch.getEsOverrideCheckBoxValue()) {
            esURLComboBox.setEnabled(true);
            esURLComboBox.setSelectedItem(elasticSearch.getSelectedEsURL());
        } else {
            esURLComboBox.setEnabled(false);
            esURLComboBox.setSelectedItem(RefreshAction.ES_URL_DEFAULT);
        }
    }

    public final ElasticSearch getElasticSearch() {
        String esURL = ((JTextField) esURLComboBox.getEditor().getEditorComponent()).getText();
        boolean esOverrideCheckBoxValue = esURLOverrideCheckBox.isSelected();
        return new ElasticSearch(esURL, esOverrideCheckBoxValue);
    }
}

