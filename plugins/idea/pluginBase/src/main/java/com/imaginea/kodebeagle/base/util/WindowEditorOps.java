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

package com.imaginea.kodebeagle.base.util;

import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.intellij.openapi.Disposable;
import com.intellij.openapi.application.Result;
import com.intellij.openapi.command.WriteCommandAction;
import com.intellij.openapi.editor.Document;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.editor.EditorFactory;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Disposer;
import com.intellij.openapi.vfs.VirtualFile;
import org.jetbrains.annotations.NotNull;

public class WindowEditorOps {
    private static WindowObjects windowObjects = WindowObjects.getInstance();

    public final void writeToDocument(final String contents,
                                      final Document windowEditorDocument) {
        new WriteCommandAction(windowObjects.getProject()) {
            @Override
            protected void run(@NotNull final Result result) throws Throwable {
                windowEditorDocument.setReadOnly(false);
                windowEditorDocument.setText(contents);
                windowEditorDocument.setReadOnly(true);
            }
        } .execute();
    }

    protected final void setWriteStatus(final VirtualFile virtualFile, final boolean status) {
        new WriteCommandAction(windowObjects.getProject()) {
            @Override
            protected void run(@NotNull final Result result) throws Throwable {
                virtualFile.setWritable(status);
            }
        } .execute();
    }

    public final void releaseEditor(final Project project, final Editor editor) {
        if (editor != null) {
            Disposer.register(project, new Disposable() {
                public void dispose() {
                    EditorFactory.getInstance().releaseEditor(editor);
                }
            });
            }
        }
}
