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

package com.imaginea.kodebeagle.impl.ui;

import com.imaginea.kodebeagle.base.ui.KBNotification;
import com.imaginea.kodebeagle.base.ui.MainWindowBase;
import com.imaginea.kodebeagle.impl.action.RefreshAction;
import com.intellij.ide.plugins.IdeaPluginDescriptor;
import com.intellij.ide.plugins.PluginManager;
import com.intellij.notification.NotificationType;
import com.intellij.openapi.actionSystem.Constraints;
import com.intellij.openapi.actionSystem.DefaultActionGroup;
import com.intellij.openapi.extensions.PluginId;
import org.jetbrains.annotations.NotNull;

public class MainWindow extends MainWindowBase {

    private static final String SCALA_DISABLED_HELP = "Scala support disabled!<br>"
            + "Please INSTALL and ENABLE scala plugin for scala support.";
    private static final String SCALA_PLUGIN_ID = "org.intellij.scala";
    private static final boolean PLUGIN_AVAILABLE =
            PluginManager.isPluginInstalled(PluginId.getId(SCALA_PLUGIN_ID));

    public static boolean scalaPluginInstalledAndEnabled() {
        if (PLUGIN_AVAILABLE) {
            IdeaPluginDescriptor descriptor =
                    PluginManager.getPlugin(PluginId.getId(SCALA_PLUGIN_ID));
            return descriptor != null && descriptor.isEnabled();
        } else {
            return false;
        }
    }

    @Override
    protected final boolean checkOptionalDependency() {
        if (!scalaPluginInstalledAndEnabled()) {
            KBNotification.getInstance().notifyBalloon(SCALA_DISABLED_HELP,
                    NotificationType.WARNING);
            return false;
        } else {
            return true;
        }
    }

    @NotNull
    @Override
    protected final DefaultActionGroup getActionGroup() {
        RefreshAction refreshAction = new RefreshAction();
        DefaultActionGroup actionGroup = getBasicActionGroup();
        actionGroup.add(refreshAction, Constraints.FIRST);
        actionGroup.addSeparator();
        return actionGroup;
    }
}
