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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

public class IdentityPanel {

    private static final String TITLE1 = "Identity";
    private static final String BEAGLE_ID = "Beagle ID:";
    private static final int[] IDENTITY_PANEL_COLUMN_WIDTHS = new int[] {0, 0, 0, 0, 99, 0};
    private static final int[] IDENTITY_PANEL_ROW_HEIGHTS = new int[] {0, 0, 0, 0};

    private static final double[] IDENTITY_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private static final double[] IDENTITY_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private JLabel beagleId;
    private JLabel beagleIdValue;
    private JPanel spacer = new JPanel(null);

    private final GridBagConstraints identityPanelVerticalSpacer =
            new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelHorizontalSpacer1 =
            new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelHorizontalSpacer2 =
            new GridBagConstraints(2, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelFirstLeft =
            new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelFirstRight =
            new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    protected IdentityPanel() {
        createFields();
    }

    public final JLabel getBeagleIdValue() {
        return beagleIdValue;
    }

    private void createFields() {
        beagleId = new JLabel(BEAGLE_ID);
        beagleIdValue = new JLabel();
    }

    public final JPanel getPanel() {
        JPanel identityPanel = new JPanel();
        identityPanel.setBorder(new TitledBorder(TITLE1));
        identityPanel.setLayout(new GridBagLayout());
        ((GridBagLayout) identityPanel.getLayout()).columnWidths = IDENTITY_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) identityPanel.getLayout()).rowHeights = IDENTITY_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) identityPanel.getLayout()).columnWeights = IDENTITY_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) identityPanel.getLayout()).rowWeights = IDENTITY_PANEL_ROW_WEIGHTS;
        identityPanel.add(spacer, identityPanelVerticalSpacer);
        identityPanel.add(spacer, identityPanelHorizontalSpacer1);
        identityPanel.add(beagleId, identityPanelFirstLeft);
        identityPanel.add(spacer, identityPanelHorizontalSpacer2);
        identityPanel.add(beagleIdValue, identityPanelFirstRight);
        return identityPanel;
    }
}
