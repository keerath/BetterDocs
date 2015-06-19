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

import java.awt.event.KeyEvent;
import javax.swing.JLabel;
import javax.swing.JTextField;

public class URLValidator extends AbstractValidator {

    private static final String NOT_URL = " is not a well formed URL!";

    public URLValidator(final String nameOfField,
                        final JTextField jTextField,
                        final JLabel validationLabel) {
        jTextField.addKeyListener(this);
        this.setjTextField(jTextField);
        this.setValidationLabel(validationLabel);
        this.setNameOfField(nameOfField);
    }

    public final boolean validateUrl(final String url) {
        boolean matches = url.matches("(http|ftp|https):\\/\\/[\\w\\-_]+"
                + "(\\.[\\w\\-_]+)+"
                + "([\\w\\-\\.,@?^=%&amp;:/~\\+#]*"
                + "[\\w\\-\\@?^=%&amp;/~\\+#])?");
        return matches;
    }

    @Override
    public void keyTyped(final KeyEvent e) {

    }

    @Override
    public void keyPressed(final KeyEvent e) {

    }

    @Override
    public final void keyReleased(final KeyEvent e) {
        if (!validateUrl(this.getjTextField().getText())) {
            if (this.getjTextField().getText().equals("")) {
                this.getValidationLabel().setText(this.getNameOfField() + NOT_EMPTY);
                this.getValidationLabel().setVisible(true);
            } else {
                this.getValidationLabel().setText(this.getNameOfField()
                        + NOT_URL);
                this.getValidationLabel().setVisible(true);
            }
        } else {
            this.getValidationLabel().setText("");
            this.getValidationLabel().setVisible(false);
        }
    }
}
