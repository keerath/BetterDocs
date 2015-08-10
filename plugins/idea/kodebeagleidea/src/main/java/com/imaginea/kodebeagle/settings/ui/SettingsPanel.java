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

import com.intellij.openapi.ui.ComboBox;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class SettingsPanel {

    private static final int[] SETTINGS_PANEL_COLUMN_WIDTHS =
            new int[] {17, 46, 36, 46, 90, 166, 0, 0, 180, 0};

    private static final int[] SETTINGS_PANEL_ROW_HEIGHTS =
            new int[] {0, 0, 0, 0, 37, 43, 55, 110, 0, 75, 0};

    private static final double[] SETTINGS_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private static final double[] SETTINGS_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private final GridBagConstraints firstPanel =
            new GridBagConstraints(1, 0, 8, 3, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints secondPanel =
            new GridBagConstraints(1, 3, 8, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints thirdPanel =
            new GridBagConstraints(1, 4, 8, 5, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints fourthPanel =
            new GridBagConstraints(1, 9, 8, 1, 0.0, 0.0,
                    GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                    new Insets(0, 0, 0, 0), 0, 0);

    private final GridBagConstraints fifthPanel =
            new GridBagConstraints(1, 10, 8, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0);

    private final IdentityPanel identityPanel = new IdentityPanel();
    private final LimitsPanel limitsPanel = new LimitsPanel();

    public final NotificationPanel getNotificationPanel() {
        return notificationPanel;
    }

    public final ElasticSearchPanel getElasticSearchPanel() {
        return elasticSearchPanel;
    }

    public final ImportsPanel getImportsPanel() {
        return importsPanel;
    }

    public final LimitsPanel getLimitsPanel() {
        return limitsPanel;
    }

    public final IdentityPanel getIdentityPanel() {
        return identityPanel;
    }

    private final ImportsPanel importsPanel = new ImportsPanel();
    private final ElasticSearchPanel elasticSearchPanel = new ElasticSearchPanel();
    private final NotificationPanel notificationPanel = new NotificationPanel();

    public final JLabel getBeagleIdValue() {
        return identityPanel.getBeagleIdValue();
    }

    public final ComboBox getEsURLComboBox() {
        return elasticSearchPanel.getEsURLComboBox();
    }

    public final PatternFilterEditor getImportsPatternFilter() {
        return importsPanel.getImportsPatternFilter();
    }

    public final JComponent createPanel() {
        GridBagLayout gridBagLayout = new GridBagLayout();
        JPanel settingsPanel = new JPanel(gridBagLayout);
        ((GridBagLayout) settingsPanel.getLayout()).columnWidths = SETTINGS_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) settingsPanel.getLayout()).rowHeights = SETTINGS_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) settingsPanel.getLayout()).columnWeights = SETTINGS_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) settingsPanel.getLayout()).rowWeights = SETTINGS_PANEL_ROW_WEIGHTS;
        settingsPanel.add(identityPanel.getPanel(), firstPanel);
        settingsPanel.add(limitsPanel.getPanel(), secondPanel);
        settingsPanel.add(importsPanel.getPanel(), thirdPanel);
        settingsPanel.add(elasticSearchPanel.getPanel(), fourthPanel);
        settingsPanel.add(notificationPanel.getPanel(), fifthPanel);
        return settingsPanel;
    }
}
