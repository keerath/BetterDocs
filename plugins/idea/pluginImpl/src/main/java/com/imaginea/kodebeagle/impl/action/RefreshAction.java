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

package com.imaginea.kodebeagle.impl.action;

import com.imaginea.kodebeagle.base.action.RefreshActionBase;
import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.imaginea.kodebeagle.base.util.UIUtils;
import com.imaginea.kodebeagle.impl.util.JavaImportsUtil;
import com.imaginea.kodebeagle.impl.util.ScalaImportsUtil;
import com.intellij.openapi.editor.Editor;

public class RefreshAction extends RefreshActionBase {
    private static final String FILETYPE_HELP_JAVA_SCALA = "<html><center>This plugin supports "
            + "\"scala\" and \"java\" files only.</center></html>";
    private static final String FILETYPE_HELP_JAVA = "<html><center>Plugin currently supports "
            + "\"java\" files only.<br> To enable scala support install scala plugin!"
            + "</center></html>";
    private static final String FILE_EXT_SCALA = "scala";
    private static final String FILE_EXT_SCALA_WORKSHEET = "sc";
    private static final String FILE_EXT_JAVA = "java";
    private final WindowObjects windowObjects = WindowObjects.getInstance();

    @Override
    protected final void runAction() {
        final Editor editor = getEditor();
        if (windowObjects.isOptionalDependencyPresent()) {
            if (checkFileType(editor.getDocument(), FILE_EXT_SCALA,
                    FILE_EXT_SCALA_WORKSHEET)) {
                windowObjects.setLanguage(FILE_EXT_SCALA);
                doQuery(new ScalaImportsUtil());
            } else if (checkFileType(editor.getDocument(), FILE_EXT_JAVA)) {
                windowObjects.setLanguage(FILE_EXT_JAVA);
                doQuery(new JavaImportsUtil());
            } else {
                UIUtils uiUtils = new UIUtils();
                uiUtils.showHelpInfo(FILETYPE_HELP_JAVA_SCALA);
            }
        } else {
            if (checkFileType(editor.getDocument(), FILE_EXT_JAVA)) {
                windowObjects.setLanguage(FILE_EXT_JAVA);
                doQuery(new JavaImportsUtil());
            } else {
                UIUtils uiUtils = new UIUtils();
                uiUtils.showHelpInfo(FILETYPE_HELP_JAVA);
            }
        }
    }
}
