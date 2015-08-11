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

import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.intellij.notification.Notification;
import com.intellij.notification.NotificationDisplayType;
import com.intellij.notification.NotificationGroup;
import com.intellij.notification.NotificationType;
import com.intellij.openapi.util.SimpleTimer;

public final class KBNotification {
    private static final KBNotification INSTANCE = new KBNotification();
    private final WindowObjects windowObjects = WindowObjects.getInstance();
    private final SimpleTimer simpleTimer = SimpleTimer.getInstance();

    private KBNotification() {
        // Restrict others from creating an INSTANCE.
    }

    private void schedule() {
        final int delayMillis = 2500;
        simpleTimer.setUp(new Runnable() {
            @Override
            public void run() {
                KBNotification.this.expire();
            }
        }, delayMillis);
    }

    public static KBNotification getInstance() {
        return INSTANCE;
    }

    private final NotificationGroup notificationsPassive = new NotificationGroup(
            RefreshAction.KODEBEAGLE, NotificationDisplayType.NONE, true);

    private final NotificationGroup notifications = new NotificationGroup(
            RefreshAction.KODEBEAGLE, NotificationDisplayType.BALLOON, true);

    private final NotificationGroup notificationsInvasive = new NotificationGroup(
            RefreshAction.KODEBEAGLE, NotificationDisplayType.STICKY_BALLOON, true);

    private Notification prevNotification;

    private void expire() {
        if (prevNotification != null) {
            prevNotification.expire();
        }
    }

    public Notification notifyInvasive(final String content,
                                             final NotificationType type) {
        expire();
        prevNotification =
                notificationsInvasive.createNotification(RefreshAction.KODEBEAGLE, content,
                        type, null);
        prevNotification.notify(windowObjects.getProject());
        return prevNotification;
    }

    public Notification notifyPassive(final String content,
                                            final NotificationType type) {
        expire();
        prevNotification =
                notificationsPassive.createNotification(RefreshAction.KODEBEAGLE, content,
                        type, null);
        prevNotification.notify(windowObjects.getProject());
        expire();
        return prevNotification;
    }

    public Notification notifyBalloon(final String content,
                                            final NotificationType type) {
        expire();
        prevNotification =
                notifications.createNotification(RefreshAction.KODEBEAGLE, content,
                        type, null);
        prevNotification.notify(windowObjects.getProject());
        schedule();
        return prevNotification;
    }

    public void error(final Throwable t) {
        expire();
        prevNotification =
                notifications.createNotification(RefreshAction.KODEBEAGLE,
                        "<i>An exception occurred while processing your request:</i>" + t.toString()
                                + "</br> Ideally it should be safe to ignore. "
                                + "However if problem persists, please report it to us by "
                                + "sending an email to kodebeagle@googlegroups.com",
                        NotificationType.ERROR, null);
        prevNotification.notify(windowObjects.getProject());
    }

}
