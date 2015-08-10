package com.imaginea.kodebeagle.model;

import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.ui.MainWindow;
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
        propertiesComponent.setValue(RefreshAction.NOTIFICATION_CHECKBOX_VALUE,
                String.valueOf(this.getNotificationsCheckBoxValue()));
        propertiesComponent.setValue(RefreshAction.LOGGING_CHECKBOX_VALUE,
                String.valueOf(this.getLoggingCheckBoxValue()));

        if (this.getNotificationsCheckBoxValue()) {
            notificationDisplayType = NotificationDisplayType.BALLOON;
        }

        if (this.getLoggingCheckBoxValue()) {
            shouldLog = true;
        }

        NotificationsConfigurationImpl.getNotificationsConfiguration().changeSettings(
                MainWindow.KODEBEAGLE, notificationDisplayType, shouldLog, false);
    }

    private void retrieve() {
        boolean isNotificationEnabled = false;
        if (NotificationsConfigurationImpl.getSettings(MainWindow.KODEBEAGLE).getDisplayType()
                != NotificationDisplayType.NONE) {
            isNotificationEnabled = true;
        }

        this.setNotificationsCheckBoxValue(isNotificationEnabled);
        this.setLoggingCheckBoxValue(NotificationsConfigurationImpl.getSettings(
                MainWindow.KODEBEAGLE).isShouldLog());
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
