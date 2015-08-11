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

package com.imaginea.kodebeagle.action;

import com.imaginea.kodebeagle.ui.KBNotification;
import com.imaginea.kodebeagle.ui.MainWindow;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.wm.ToolWindowManager;
import java.io.IOException;

public class SearchKodeBeagleAction extends AnAction {

    public final void actionPerformed(final AnActionEvent e) {
        Project project = e.getProject();
        if (project != null) {
            ToolWindowManager.getInstance(e.getProject()).
                    getToolWindow(MainWindow.KODEBEAGLE).show(new Runnable() {
                @Override
                public void run() {
                    try {
                        new RefreshAction().init();
                    } catch (IOException ioe) {
                        KBNotification.getInstance().error(ioe);
                        ioe.printStackTrace();
                    }
                }
            });
        }
    }
}
