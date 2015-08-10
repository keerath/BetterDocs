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
import com.imaginea.kodebeagle.model.Limits;
import com.intellij.util.ui.UIUtil;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseEvent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.plaf.metal.MetalSliderUI;

@SuppressWarnings("PMD")
public class LimitsPanel {

    private static final String TITLE2 = "Limits";
    private static final String RESULTS_SIZE = "Result Size:";
    private static final String DISTANCE_FROM_CURSOR = "Lines from cursor:";
    private static final String SPOTLIGHT_RESULT_COUNT = "Spotlight Results Count:";
    public static final int LINES_FROM_CURSOR_MIN = 0;
    public static final int LINES_FROM_CURSOR_MAX = 50;
    private static final int MAJOR_TICK_SPACING = 25;
    private static final int MINOR_TICK_SPACING = 1;
    private static final int STEP_SIZE = 5;
    public static final int MIN = 5;
    public static final int RESULT_SIZE_MAX = 50;
    public static final int TOP_COUNT_MAX = 20;

    private static final int[] LIMITS_PANEL_COLUMN_WIDTHS =
            new int[] {29, 0, 0, 0, 0, 0, 37, 25, 41, 0, 0, 0, 0, 0};

    private static final int[] LIMITS_PANEL_ROW_HEIGHTS =
            new int[] {0, 0, 0, 0, 0, 0, 0, 0};

    private static final double[] LIMITS_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private static final double[] LIMITS_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0E-4};

    private final GridBagConstraints limitsPanelVerticalSpacer1 =
            new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelHorizontalSpacer1 =
            new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelFirstLeft =
            new GridBagConstraints(1, 1, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelHorizontalSpacer2 =
            new GridBagConstraints(4, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelFirstCenter =
            new GridBagConstraints(5, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelFirstRight =
            new GridBagConstraints(6, 1, 6, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints limitsPanelVerticalSpacer2 =
            new GridBagConstraints(3, 2, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelSecondLeft =
            new GridBagConstraints(1, 3, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelHorizontalSpacer3 =
            new GridBagConstraints(4, 3, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelSecondRight =
            new GridBagConstraints(5, 3, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelVerticalSpacer3 =
            new GridBagConstraints(3, 4, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelThirdLeft =
            new GridBagConstraints(1, 5, 3, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelHorizontalSpacer4 =
            new GridBagConstraints(4, 5, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelThirdRight =
            new GridBagConstraints(5, 5, 2, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints limitsPanelVerticalSpacer4 =
            new GridBagConstraints(3, 6, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 0, 5), 0, 0);

    private final JPanel spacer = new JPanel(null);
    private JSpinner resultSizeSpinner;
    private JSlider linesFromCursorSlider;
    private JSpinner topCountSpinner;
    private JTextField linesFromCursorSliderFeedBack;

    protected LimitsPanel() {
        createFields();
    }

    public final JSpinner getResultSizeSpinner() {
        return resultSizeSpinner;
    }

    public final JSlider getLinesFromCursorSlider() {
        return linesFromCursorSlider;
    }

    public final JSpinner getTopCountSpinner() {
        return topCountSpinner;
    }

    private void createFields() {
        linesFromCursorSlider = new JSlider(JSlider.HORIZONTAL, LINES_FROM_CURSOR_MIN,
                LINES_FROM_CURSOR_MAX, RefreshAction.LINES_FROM_CURSOR_DEFAULT_VALUE);
        linesFromCursorSlider.setMajorTickSpacing(MAJOR_TICK_SPACING);
        linesFromCursorSlider.setMinorTickSpacing(MINOR_TICK_SPACING);
        linesFromCursorSlider.setPaintLabels(true);
        linesFromCursorSlider.setSnapToTicks(true);
        linesFromCursorSlider.setUI(new MoveOnClickUI());
        UIUtil.setSliderIsFilled(linesFromCursorSlider, true);
        linesFromCursorSliderFeedBack = new JTextField();
        linesFromCursorSliderFeedBack.setVisible(true);
        linesFromCursorSliderFeedBack.setEditable(false);
        linesFromCursorSliderFeedBack.setText(String.valueOf(linesFromCursorSlider.getValue()));
        linesFromCursorSlider.addChangeListener(new FeedBackListener(
                linesFromCursorSliderFeedBack));

        resultSizeSpinner = new JSpinner();
        resultSizeSpinner.setModel(new SpinnerNumberModel(
                RefreshAction.SIZE_DEFAULT_VALUE, MIN, RESULT_SIZE_MAX, STEP_SIZE));
        resultSizeSpinner.setVisible(true);
        ((JSpinner.DefaultEditor) resultSizeSpinner.getEditor()).getTextField().setEditable(false);

        topCountSpinner = new JSpinner();
        topCountSpinner.setModel(new SpinnerNumberModel(
                RefreshAction.TOP_COUNT_DEFAULT_VALUE, MIN, TOP_COUNT_MAX, STEP_SIZE));
        topCountSpinner.setVisible(true);
        ((JSpinner.DefaultEditor) topCountSpinner.getEditor()).getTextField().setEditable(false);
    }

    public final JPanel getPanel() {
        createFields();
        JPanel limitsPanel = new JPanel();
        limitsPanel.setBorder(new TitledBorder(TITLE2));
        limitsPanel.setLayout(new GridBagLayout());
        ((GridBagLayout) limitsPanel.getLayout()).columnWidths = LIMITS_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) limitsPanel.getLayout()).rowHeights = LIMITS_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) limitsPanel.getLayout()).columnWeights = LIMITS_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) limitsPanel.getLayout()).rowWeights = LIMITS_PANEL_ROW_WEIGHTS;
        limitsPanel.add(spacer,  limitsPanelVerticalSpacer1);
        limitsPanel.add(spacer, limitsPanelHorizontalSpacer1);
        limitsPanel.add(new JLabel(DISTANCE_FROM_CURSOR), limitsPanelFirstLeft);
        limitsPanel.add(spacer, limitsPanelHorizontalSpacer2);
        limitsPanel.add(linesFromCursorSliderFeedBack, limitsPanelFirstCenter);
        limitsPanel.add(linesFromCursorSlider, limitsPanelFirstRight);
        limitsPanel.add(spacer, limitsPanelVerticalSpacer2);
        limitsPanel.add(new JLabel(RESULTS_SIZE), limitsPanelSecondLeft);
        limitsPanel.add(spacer, limitsPanelHorizontalSpacer3);
        limitsPanel.add(resultSizeSpinner, limitsPanelSecondRight);
        limitsPanel.add(spacer, limitsPanelVerticalSpacer3);
        limitsPanel.add(new JLabel(SPOTLIGHT_RESULT_COUNT), limitsPanelThirdLeft);
        limitsPanel.add(spacer, limitsPanelHorizontalSpacer4);
        limitsPanel.add(topCountSpinner, limitsPanelThirdRight);
        limitsPanel.add(spacer, limitsPanelVerticalSpacer4);
        return limitsPanel;
    }

    private static class MoveOnClickUI extends MetalSliderUI {
        @Override
        protected TrackListener createTrackListener(final JSlider jslider) {
            return new TrackListener() {
                @Override
                public void mousePressed(final MouseEvent e) {
                    if (e.getSource() instanceof JSlider) {
                        JSlider jSlider = (JSlider) e.getSource();
                        jSlider.setValue(valueForXPosition(e.getX()));
                        super.mousePressed(e);
                        super.mouseDragged(e);
                    }
                }
                @Override
                public boolean shouldScroll(final int direction) {
                    return false;
                }
            };
        }
    }

    private static class FeedBackListener implements ChangeListener {

        private final JTextField feedBackTextField;
        public FeedBackListener(final JTextField pFeedBackTextField) {
            this.feedBackTextField = pFeedBackTextField;
        }

        @Override
        public void stateChanged(final ChangeEvent e) {
            if (e.getSource() instanceof  JSlider) {
                JSlider jSlider = (JSlider) e.getSource();
                feedBackTextField.setText(String.valueOf(jSlider.getValue()));
                feedBackTextField.requestFocus();
            }
        }
    }

    public final void reset(final Limits limits) {
        linesFromCursorSlider.setValue(limits.getLinesFromCursor());
        resultSizeSpinner.setValue(limits.getResultSize());
        topCountSpinner.setValue(limits.getTopCount());
    }

    public final Limits getLimits() {
        int linesFromCursorValue = linesFromCursorSlider.getValue();
        int sizeValue = (int) resultSizeSpinner.getValue();
        int topCountValue = (int) topCountSpinner.getValue();
        return new Limits(linesFromCursorValue, sizeValue, topCountValue);
    }
}

