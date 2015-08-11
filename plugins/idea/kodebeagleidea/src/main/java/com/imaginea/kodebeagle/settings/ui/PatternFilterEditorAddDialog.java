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

import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.DialogWrapper;
import com.intellij.ui.UIBundle;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import org.jetbrains.annotations.Nullable;


class PatternFilterEditorAddDialog extends DialogWrapper {
    private static final int NUM_OF_COLUMNS = 35;
    private static final GridBagConstraints HEADER_LABEL =
            new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.NORTHWEST,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 0, 0), 0, 0);

    private static final GridBagConstraints TEXT_FIELD =
            new GridBagConstraints(1, 1, 1, 1, 1.0, 1.0, GridBagConstraints.NORTHWEST,
                    GridBagConstraints.HORIZONTAL, new Insets(5, 10, 0, 0), 0, 0);


    private final String myHelpId;
    private JTextField patternField;
    private String patternValue;

    public PatternFilterEditorAddDialog(final Project project, final String helpId) {
        super(project, true);
        myHelpId = helpId;
        setTitle(UIBundle.message("class.filter.editor.add.dialog.title"));
        init();
    }

    protected JComponent createCenterPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final JLabel header = new JLabel(UIBundle.message("label.class.filter.editor."
                + "add.dialog.filter.pattern"));
        patternField = new JTextField(NUM_OF_COLUMNS);
        panel.add(header, HEADER_LABEL);
        panel.add(patternField, TEXT_FIELD);
        return panel;
    }

    public final String getPattern() {
        return patternValue;
    }

    public final void setPattern(final String pattern) {
        this.patternValue = pattern;
    }


    @Override
    protected final void doOKAction() {
        super.doOKAction();
        this.setPattern(patternField.getText());
    }

    @Override @Nullable
    protected String getHelpId() {
        return myHelpId;
    }
}
