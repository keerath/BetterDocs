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

package com.imaginea.kodebeagle.base.model;

import com.imaginea.kodebeagle.base.action.RefreshActionBase;
import com.imaginea.kodebeagle.base.ui.MainWindowBase;
import com.intellij.ide.util.PropertiesComponent;
import com.intellij.notification.NotificationDisplayType;
import com.intellij.notification.impl.NotificationsConfigurationImpl;

public class Notifications {

    private static final int PRIME = 31;
    private boolean notificationsCheckBoxValue;
    private boolean loggingCheckBoxValue;
    private final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

    public Notifications(final boolean pNotificationCheckBoxValue,
                         final boolean pLoggingCheckBoxValue) {
        this.notificationsCheckBoxValue = pNotificationCheckBoxValue;
        this.loggingCheckBoxValue = pLoggingCheckBoxValue;
    }

    public Notifications() {
        retrieve();
    }


    public final boolean getNotificationsCheckBoxValue() {
        return notificationsCheckBoxValue;
    }

    public final void setNotificationsCheckBoxValue(final boolean pNotificationsCheckBoxValue) {
        this.notificationsCheckBoxValue = pNotificationsCheckBoxValue;
    }

    public final boolean getLoggingCheckBoxValue() {
        return loggingCheckBoxValue;
    }

    public final void setLoggingCheckBoxValue(final boolean pLoggingCheckBoxValue) {
        this.loggingCheckBoxValue = pLoggingCheckBoxValue;
    }

    public final void save() {
        NotificationDisplayType notificationDisplayType = NotificationDisplayType.NONE;

        boolean shouldLog = false;
        propertiesComponent.setValue(RefreshActionBase.NOTIFICATION_CHECKBOX_VALUE,
                String.valueOf(this.getNotificationsCheckBoxValue()));
        propertiesComponent.setValue(RefreshActionBase.LOGGING_CHECKBOX_VALUE,
                String.valueOf(this.getLoggingCheckBoxValue()));

        if (this.getNotificationsCheckBoxValue()) {
            notificationDisplayType = NotificationDisplayType.BALLOON;
        }

        if (this.getLoggingCheckBoxValue()) {
            shouldLog = true;
        }

        NotificationsConfigurationImpl.getNotificationsConfiguration().changeSettings(
                MainWindowBase.KODEBEAGLE, notificationDisplayType, shouldLog, false);
    }

    private void retrieve() {
        boolean isNotificationEnabled = false;
        if (NotificationsConfigurationImpl.getSettings(MainWindowBase.KODEBEAGLE).getDisplayType()
                != NotificationDisplayType.NONE) {
            isNotificationEnabled = true;
        }

        this.setNotificationsCheckBoxValue(isNotificationEnabled);
        this.setLoggingCheckBoxValue(NotificationsConfigurationImpl.getSettings(
                MainWindowBase.KODEBEAGLE).isShouldLog());
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == this) {
            return  true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        Notifications myNotifications = (Notifications) obj;
        return this.getNotificationsCheckBoxValue()
                == myNotifications.getNotificationsCheckBoxValue()
                && this.getLoggingCheckBoxValue()
                == myNotifications.getLoggingCheckBoxValue();
    }

    @Override
    public final int hashCode() {
        int hashCode = 0;
        hashCode = PRIME * Boolean.valueOf(notificationsCheckBoxValue).hashCode() + hashCode;
        hashCode = PRIME * Boolean.valueOf(loggingCheckBoxValue).hashCode() + hashCode;
        return hashCode;
    }
}
