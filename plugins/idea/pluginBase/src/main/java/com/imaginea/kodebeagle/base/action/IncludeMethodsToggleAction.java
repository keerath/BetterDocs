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

package com.imaginea.kodebeagle.base.action;

import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.intellij.icons.AllIcons;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.ToggleAction;

public class IncludeMethodsToggleAction extends ToggleAction {

    private static final String INCLUDE_METHODS_TEXT = "Include method calls";
    private static final String INCLUDE_METHODS_DESC = "Include method calls in your search";
    private final WindowObjects windowObjects = WindowObjects.getInstance();

    public IncludeMethodsToggleAction() {
        super(INCLUDE_METHODS_TEXT, INCLUDE_METHODS_DESC, AllIcons.Nodes.Method);
    }

    @Override
    public final boolean isSelected(final AnActionEvent e) {
       return windowObjects.retrieveIncludeMethods();
    }

    @Override
    public final void setSelected(final AnActionEvent e, final boolean state) {
        windowObjects.saveIncludeMethods(state);
    }
}
