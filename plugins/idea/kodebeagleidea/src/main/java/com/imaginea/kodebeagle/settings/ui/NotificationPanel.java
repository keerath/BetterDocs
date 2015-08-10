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

import com.imaginea.kodebeagle.model.Notifications;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import javax.swing.JCheckBox;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

public class NotificationPanel {

    private static final String TITLE4 = "Notifications";
    private static final String ENABLE_NOTIFICATION = "Enable notifications";
    private static final String ENABLE_LOGGING = "Enable logging";
    private static final int[] NOTIFICATION_PANEL_COLUMN_WIDTHS = new int[] {32, 73, 28, 62, 0};
    private static final int[] NOTIFICATION_PANEL_ROW_HEIGHTS = new int[] {0, 22, 0, 0};
    private static final double[] NOTIFICATION_PANEL_COLUMN_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 0.0, 1.0E-4};
    private static final double[] NOTIFICATION_PANEL_ROW_WEIGHTS =
            new double[] {0.0, 0.0, 0.0, 1.0E-4};

    private final GridBagConstraints notificationPanelVerticalSpacer1 =
            new GridBagConstraints(3, 0, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints notificationPanelHorizontalSpacer1 =
            new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 5), 0, 0);

    private final GridBagConstraints notificationPanelFirstLeft =
            new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final GridBagConstraints notificationPanelFirstRight =
            new GridBagConstraints(3, 1, 1, 1, 0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.BOTH, new Insets(0, 0, 5, 0), 0, 0);

    private final JPanel spacer = new JPanel(null);
    private JCheckBox notificationCheckBox;
    private JCheckBox loggingCheckBox;

    public final JCheckBox getNotificationCheckBox() {
        return notificationCheckBox;
    }

    public final JCheckBox getLoggingCheckBox() {
        return loggingCheckBox;
    }

    public NotificationPanel() {
        createFields();
    }

    private void createFields() {
        notificationCheckBox = new JCheckBox(ENABLE_NOTIFICATION);
        loggingCheckBox = new JCheckBox(ENABLE_LOGGING);
    }

    public final JPanel getPanel() {
        JPanel notificationPanel = new JPanel();
        notificationPanel.setBorder(new TitledBorder(TITLE4));
        notificationPanel.setLayout(new GridBagLayout());
        ((GridBagLayout) notificationPanel.getLayout()).columnWidths =
                NOTIFICATION_PANEL_COLUMN_WIDTHS;
        ((GridBagLayout) notificationPanel.getLayout()).rowHeights =
                NOTIFICATION_PANEL_ROW_HEIGHTS;
        ((GridBagLayout) notificationPanel.getLayout()).columnWeights =
                NOTIFICATION_PANEL_COLUMN_WEIGHTS;
        ((GridBagLayout) notificationPanel.getLayout()).rowWeights =
                NOTIFICATION_PANEL_ROW_WEIGHTS;
        notificationPanel.add(spacer, notificationPanelVerticalSpacer1);
        notificationPanel.add(notificationCheckBox, notificationPanelFirstLeft);
        notificationPanel.add(spacer, notificationPanelHorizontalSpacer1);
        notificationPanel.add(loggingCheckBox, notificationPanelFirstRight);
        return notificationPanel;
    }

    public final void reset(final Notifications notifications) {
        notificationCheckBox.setSelected(
                notifications.getNotificationsCheckBoxValue());
        loggingCheckBox.setSelected(notifications.getLoggingCheckBoxValue());
    }

    public final Notifications getNotifications() {
        boolean notificationCheckBoxValue = notificationCheckBox.isSelected();
        boolean loggingCheckBoxValue = loggingCheckBox.isSelected();
        return new Notifications(notificationCheckBoxValue, loggingCheckBoxValue);
    }
}
