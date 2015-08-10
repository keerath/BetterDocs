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

import com.imaginea.kodebeagle.model.Identity;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

@SuppressWarnings("PMD")
public class IdentityPanel {

    private static final String TITLE1 = "SettingsConfigurable";
    private static final String BEAGLE_ID = "Beagle ID:";
    private static final String OPT_OUT = "Opt-Out";
    private static final String HELP_TEXT_1 = "* Beagle ID is used to anonymously track"
            + " search queries for improving your search experience.";
    private static final String HELP_TEXT_2 = "* By selecting Opt-Out mode we DON'T"
            + " track your search queries.";
    private static final String HELP_TEXT_3 = " (For further details refer our Privacy Policy)";
    private static final String HELP_FONT_NAME = "Sans Serif";
    private static final int HELP_FONT_SIZE = 12;
    private static final int[] IDENTITY_PANEL_COLUMN_WIDTHS =
            new int[] {29, 87, 126, 0, 0, 104, 0, 0, 0, 0, 0, 0, 0, 0};

    private static final int[] IDENTITY_PANEL_ROW_HEIGHTS =
            new int[] {0, 0, 0, 0, 0, 0, 0};

    private static final double[] IDENTITY_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private static final double[] IDENTITY_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private JLabel beagleId;
    private JLabel beagleIdValue;
    private JLabel beagleIdHelp;
    private JLabel optOutHelp;
    private JLabel privacyHelp;
    private JCheckBox optOut;
    private final JPanel spacer = new JPanel(null);

    private final GridBagConstraints identityPanelVerticalSpacer1 =
            new GridBagConstraints(1, 0, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelVerticalSpacer2 =
            new GridBagConstraints(1, 2, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
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

    private final GridBagConstraints identityPanelFirstCenter =
            new GridBagConstraints(2, 1, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelFirstRight =
            new GridBagConstraints(5, 1, 1, 1, 0.0, 0.0,
                    GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                    new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelSecondLeft =
            new GridBagConstraints(1, 3, 12, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints identityPanelThirdLeft =
            new GridBagConstraints(1, 4, 10, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints identityPanelFourthLeft =
            new GridBagConstraints(1, 5, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 5), 0, 0);

    protected IdentityPanel() {
        createFields();
    }

    public final JLabel getBeagleIdValue() {
        return beagleIdValue;
    }

    public final JCheckBox getOptOut() {
        return optOut;
    }

    private void createFields() {
        beagleId = new JLabel(BEAGLE_ID);
        beagleIdValue = new JLabel();
        optOut = new JCheckBox(OPT_OUT);
        beagleIdHelp = new JLabel(HELP_TEXT_1);
        beagleIdHelp.setFont(new Font(HELP_FONT_NAME, Font.ITALIC, HELP_FONT_SIZE));
        optOutHelp = new JLabel(HELP_TEXT_2);
        optOutHelp.setFont(new Font(HELP_FONT_NAME, Font.ITALIC, HELP_FONT_SIZE));
        privacyHelp = new JLabel(HELP_TEXT_3);
        privacyHelp.setFont(new Font(HELP_FONT_NAME, Font.ITALIC, HELP_FONT_SIZE));
    }

    public final JPanel getPanel() {
        JPanel identityPanel = new JPanel();
        identityPanel.setBorder(new TitledBorder(TITLE1));
        identityPanel.setLayout(new GridBagLayout());
        ((GridBagLayout) identityPanel.getLayout()).columnWidths = IDENTITY_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) identityPanel.getLayout()).rowHeights = IDENTITY_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) identityPanel.getLayout()).columnWeights = IDENTITY_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) identityPanel.getLayout()).rowWeights = IDENTITY_PANEL_ROW_WEIGHTS;
        identityPanel.add(spacer, identityPanelVerticalSpacer1);
        identityPanel.add(spacer, identityPanelHorizontalSpacer1);
        identityPanel.add(beagleId, identityPanelFirstLeft);
        identityPanel.add(spacer, identityPanelHorizontalSpacer2);
        identityPanel.add(beagleIdValue, identityPanelFirstCenter);
        identityPanel.add(optOut, identityPanelFirstRight);
        identityPanel.add(spacer, identityPanelVerticalSpacer2);
        identityPanel.add(beagleIdHelp, identityPanelSecondLeft);
        identityPanel.add(optOutHelp, identityPanelThirdLeft);
        identityPanel.add(privacyHelp, identityPanelFourthLeft);
        return identityPanel;
    }

    public final void reset(final Identity identity) {
        identity.loadBeagleId();
        beagleIdValue.setText(identity.getBeagleIdValue());
        optOut.setSelected(identity.getOptOutCheckBoxValue());
    }

    public final Identity getIdentity() {
        boolean optOutCheckBoxValue = optOut.isSelected();
        return new Identity(optOutCheckBoxValue);
    }
}
