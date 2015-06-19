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

public class IntegerValidator extends AbstractValidator {

    private static final String ONLY_DIGITS = " can have only digits [0-9]!";
    private static final String LESS_THAN_LIMIT = " should be less than ";

    public IntegerValidator(final String nameOfField,
                            final JTextField jTextField,
                            final JLabel validationLabel, final int upperLimit) {

        jTextField.addKeyListener(this);
        this.setjTextField(jTextField);
        this.setNameOfField(nameOfField);
        this.setValidationLabel(validationLabel);
        this.setUpperLimit(upperLimit);
    }


    private boolean validateInteger(final String integer) {
        return integer.matches("^\\d+$");
    }

    private boolean lessThanUpperLimit(final String integer, final int upperLimit) {
        return Integer.parseInt(integer) <= upperLimit;

    }

    @Override
    public void keyTyped(final KeyEvent e) {

    }

    @Override
    public void keyPressed(final KeyEvent e) {

    }

    @Override
    public final void keyReleased(final KeyEvent e) {
        if (!validateInteger(this.getjTextField().getText())) {
            if (this.getjTextField().getText().isEmpty()) {
                this.getValidationLabel().setText(this.getNameOfField() + NOT_EMPTY);
                this.getValidationLabel().setVisible(true);
            } else {
                this.getValidationLabel().setText(this.getNameOfField()
                        + ONLY_DIGITS);
                this.getValidationLabel().setVisible(true);
            }
        } else if (!lessThanUpperLimit(this.getjTextField().getText(), this.getUpperLimit())) {
            this.getValidationLabel().setText(this.getNameOfField()
                    + LESS_THAN_LIMIT + this.getUpperLimit());
            this.getValidationLabel().setVisible(true);

        } else {
            this.getValidationLabel().setText("");
            this.getValidationLabel().setVisible(false);
        }
    }
}
