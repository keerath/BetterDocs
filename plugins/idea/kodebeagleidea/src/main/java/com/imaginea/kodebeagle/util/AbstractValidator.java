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

package com.imaginea.kodebeagle.util;

import java.awt.event.KeyListener;
import javax.swing.JLabel;
import javax.swing.JTextField;

public abstract class AbstractValidator implements KeyListener {
    protected static final String NOT_EMPTY = " can't be empty!";
    private JTextField jTextField;
    private JLabel validationLabel;
    private String nameOfField;
    private int upperLimit;


    public final JTextField getjTextField() {
        return jTextField;
    }

    public final void setjTextField(final JTextField pJTextField) {
        this.jTextField = pJTextField;
    }

    public final JLabel getValidationLabel() {
        return validationLabel;
    }

    public final void setValidationLabel(final JLabel pValidationLabel) {
        this.validationLabel = pValidationLabel;
    }

    public final String getNameOfField() {
        return nameOfField;
    }

    public final void setNameOfField(final String pNameOfField) {
        this.nameOfField = pNameOfField;
    }

    public final int getUpperLimit() {
        return upperLimit;
    }

    public final void setUpperLimit(final int pUpperLimit) {
        this.upperLimit = pUpperLimit;
    }
}
