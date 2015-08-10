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

import com.imaginea.kodebeagle.model.Imports;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.intellij.ui.classFilter.ClassFilter;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.List;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

public class ImportsPanel {


    private static final String TITLE3 = "Configure Imports";
    private static final String EXCLUDE_IMPORTS = "Exclude the following imports:";

    private static final int[] IMPORTS_PANEL_COLUMN_WIDTHS =
            new int[] {29, 48, 0, 91, 218, 53, 55, 17, 24, 0};

    private static final int[] IMPORTS_PANEL_ROW_HEIGHTS =
            new int[] {0, 0, 40, 0, 31, 35, 36, 11, 0};

    private static final double[] IMPORTS_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private static final double[] IMPORTS_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private final GridBagConstraints importsPanelFirstLeft =
            new GridBagConstraints(1, 1, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints importsPanelVerticalSpacer1 =
            new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints importsPanelSecondLeft =
            new GridBagConstraints(1, 2, 7, 6, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 5), 0, 0);

    private final GridBagConstraints importsPanelVerticalSpacer2 =
            new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints importsPanelHorizontalSpacer1 =
            new GridBagConstraints(8, 3, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final JPanel spacer = new JPanel(null);
    private JCheckBox excludeImportsCheckBox;
    private PatternFilterEditor importsPatternFilter;
    private WindowObjects windowObjects = WindowObjects.getWindowObjects();

    protected ImportsPanel() {
        createFields();
    }

    public final JCheckBox getExcludeImportsCheckBox() {
        return excludeImportsCheckBox;
    }

    public final PatternFilterEditor getImportsPatternFilter() {
        return importsPatternFilter;
    }

    private void createFields() {
        importsPatternFilter = new PatternFilterEditor(windowObjects.getProject());
        excludeImportsCheckBox = new JCheckBox();
        excludeImportsCheckBox.setText(EXCLUDE_IMPORTS);
        excludeImportsCheckBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                if (importsPatternFilter.isEnabled()) {
                    importsPatternFilter.setEnabled(false);
                } else {
                    importsPatternFilter.setEnabled(true);
                    importsPatternFilter.requestFocus();
                }
            }
        });
    }

    public final JPanel getPanel() {
        JPanel importsPanel = new JPanel();
        importsPanel.setBorder(new TitledBorder(TITLE3));
        importsPanel.setLayout(new GridBagLayout());
        ((GridBagLayout) importsPanel.getLayout()).columnWidths = IMPORTS_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) importsPanel.getLayout()).rowHeights = IMPORTS_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) importsPanel.getLayout()).columnWeights = IMPORTS_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) importsPanel.getLayout()).rowWeights = IMPORTS_PANEL_ROW_WEIGHTS;
        importsPanel.add(spacer, importsPanelVerticalSpacer1);
        importsPanel.add(excludeImportsCheckBox, importsPanelFirstLeft);
        importsPanel.add(spacer, importsPanelVerticalSpacer2);
        importsPanel.add(importsPatternFilter, importsPanelSecondLeft);
        importsPanel.add(spacer, importsPanelHorizontalSpacer1);
        return importsPanel;
    }

    public final void reset(final Imports imports) {
        excludeImportsCheckBox.setSelected(
                imports.getExcludeImportsCheckBoxValue());
        List<ClassFilter> filtersList = imports.getFilterList();
        importsPatternFilter.setFilters(filtersList.toArray(new ClassFilter[filtersList.size()]));
        if (imports.getExcludeImportsCheckBoxValue()) {
            importsPatternFilter.setEnabled(true);
        } else {
            importsPatternFilter.setEnabled(false);
        }
    }

    public final Imports getImports() {
        List<ClassFilter> filtersList = Arrays.asList(importsPatternFilter.getFilters());
        boolean excludeImportsCheckBoxValue = excludeImportsCheckBox.isSelected();
        return new Imports(filtersList, excludeImportsCheckBoxValue);
    }
}
