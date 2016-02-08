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

package com.imaginea.kodebeagle.base.model;

import com.imaginea.kodebeagle.base.action.RefreshActionBase;
import com.imaginea.kodebeagle.base.settings.ui.LimitsPanel;
import com.intellij.ide.util.PropertiesComponent;

public class Limits {

    private static final int PRIME = 31;
    private int linesFromCursor;
    private int resultSize;
    private int topCount;
    private final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

    public Limits() { retrieve(); }

    public Limits(final int pLinesFromCursor, final int pResultSize, final int pTopCount) {
        this.linesFromCursor = pLinesFromCursor;
        this.resultSize = pResultSize;
        this.topCount = pTopCount;
    }

    public final int getLinesFromCursor() {
        return linesFromCursor;
    }

    public final int getResultSize() {
        return resultSize;
    }

    public final int getTopCount() {
        return topCount;
    }

    public final void setLinesFromCursor(final int pLinesFromCursor) {
        this.linesFromCursor = pLinesFromCursor;
    }

    public final void setResultSize(final int pResultSize) {
        this.resultSize = pResultSize;
    }

    public final void setTopCount(final int pTopCount) {
        this.topCount = pTopCount;
    }

    public final void save() {
        propertiesComponent.setValue(RefreshActionBase.LINES_FROM_CURSOR,
                String.valueOf(this.getLinesFromCursor()));
        propertiesComponent.setValue(RefreshActionBase.SIZE,
                String.valueOf(this.getResultSize()));
        propertiesComponent.setValue(RefreshActionBase.TOP_COUNT,
                String.valueOf(this.getTopCount()));
    }


    private int getValueBasedOnLimits(final int persistedValue, final int min,
                                      final int max, final int defaultValue) {
        if (min <= persistedValue && persistedValue <= max) {
            return persistedValue;
        } else {
            return defaultValue;
        }
    }

    private void retrieve() {
        int persistedLinesFromCursor = Integer.parseInt(propertiesComponent.getValue(
                RefreshActionBase.LINES_FROM_CURSOR,
                String.valueOf(RefreshActionBase.LINES_FROM_CURSOR_DEFAULT_VALUE)));
        int persistedResultSize = Integer.parseInt(propertiesComponent.getValue(
                RefreshActionBase.SIZE,
                String.valueOf(RefreshActionBase.SIZE_DEFAULT_VALUE)));
        int persistedTopCount = Integer.parseInt(propertiesComponent.getValue(
                RefreshActionBase.TOP_COUNT,
                String.valueOf(RefreshActionBase.TOP_COUNT_DEFAULT_VALUE)));
        this.setLinesFromCursor(getValueBasedOnLimits(persistedLinesFromCursor,
                LimitsPanel.LINES_FROM_CURSOR_MIN, LimitsPanel.LINES_FROM_CURSOR_MAX,
                RefreshActionBase.LINES_FROM_CURSOR_DEFAULT_VALUE));
        this.setResultSize(getValueBasedOnLimits(persistedResultSize, LimitsPanel.MIN,
                LimitsPanel.RESULT_SIZE_MAX, RefreshActionBase.SIZE_DEFAULT_VALUE));
        this.setTopCount(getValueBasedOnLimits(persistedTopCount, LimitsPanel.MIN,
                LimitsPanel.TOP_COUNT_MAX, RefreshActionBase.TOP_COUNT_DEFAULT_VALUE));
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        Limits limits = (Limits) obj;
        return this.getLinesFromCursor() == limits.getLinesFromCursor()
                && this.getResultSize() == limits.getResultSize()
                && this.getTopCount() == limits.getTopCount();
    }

    @Override
    public final int hashCode() {
        int hashCode = 0;
        hashCode = PRIME * linesFromCursor + hashCode;
        hashCode = PRIME * resultSize + hashCode;
        hashCode = PRIME * topCount + hashCode;
        return hashCode;
    }
}
