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

package com.imaginea.kodebeagle.tasks;

import com.imaginea.kodebeagle.model.CodeInfo;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.util.ESUtils;
import com.imaginea.kodebeagle.util.EditorDocOps;
import com.imaginea.kodebeagle.util.WindowEditorOps;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.progress.PerformInBackgroundOption;
import com.intellij.openapi.progress.ProgressIndicator;
import com.intellij.openapi.progress.Task;
import com.intellij.openapi.project.Project;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class FetchFileContentTask extends Task.Backgroundable {

    private static final String KODE_BEAGLE = "KodeBeagle";
    private static final String FETCHING_FILE_CONTENT = "Fetching file content ...";
    private final CodeInfo codeInfo;
    private String fileContents;
    private WindowObjects windowObjects = WindowObjects.getInstance();
    private WindowEditorOps windowEditorOps = new WindowEditorOps();
    private EditorDocOps editorDocOps = new EditorDocOps();
    private ESUtils esUtils = new ESUtils();

    public FetchFileContentTask(final Project project, final CodeInfo pCodeInfo) {
        super(project, KODE_BEAGLE, true, PerformInBackgroundOption.ALWAYS_BACKGROUND);
        this.codeInfo = pCodeInfo;
    }

    @Override
    public final void run(@NotNull final ProgressIndicator indicator) {
        indicator.setText(FETCHING_FILE_CONTENT);
        indicator.setFraction(0.0);
        String fileName = codeInfo.getAbsoluteFileName();
        fileContents = esUtils.getContentsForFile(fileName);

        //Setting contents so that we can use that for Open in New Tab
        codeInfo.setContents(fileContents);
        indicator.setFraction(1.0);
    }

    @Override
    public final void onSuccess() {
        updateMainPanePreviewEditor(codeInfo.getLineNumbers());
    }

    private void updateMainPanePreviewEditor(final List<Integer> lineNumbers) {
        final Document mainPanePreviewEditorDocument =
                windowObjects.getWindowEditor().getDocument();
        if (fileContents != null) {
            String contentsInLines =
                    editorDocOps.getContentsInLines(fileContents, lineNumbers);
            windowEditorOps.writeToDocument(contentsInLines, mainPanePreviewEditorDocument);
        }
    }
}
