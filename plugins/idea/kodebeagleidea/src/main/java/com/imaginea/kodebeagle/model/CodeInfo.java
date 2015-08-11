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

package com.imaginea.kodebeagle.model;

import java.util.List;

public class CodeInfo {
    private List<Integer> lineNumbers;
    private String contents;
    private String absoluteFileName;

    public final List<Integer> getLineNumbers() {
        return this.lineNumbers;
    }

    public final String getContents() {
        return this.contents;
    }

    public final String getAbsoluteFileName() {
        return absoluteFileName;
    }

    public CodeInfo(final String pfileName, final List<Integer> plineNumbers) {
        this.absoluteFileName = pfileName;
        this.lineNumbers = plineNumbers;
    }

    public final void setContents(final String pcontents) {
        this.contents = pcontents;
    }

    public final String toString() {
        return absoluteFileName.substring(absoluteFileName.lastIndexOf("/") + 1);
    }

    public final String getDisplayFileName() {
        return absoluteFileName.substring(absoluteFileName.lastIndexOf("/") + 1);
    }

}

