package com.imaginea.kodebeagle.model;

import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.settings.ui.LimitsPanel;
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
        propertiesComponent.setValue(RefreshAction.LINES_FROM_CURSOR,
                String.valueOf(this.getLinesFromCursor()));
        propertiesComponent.setValue(RefreshAction.SIZE,
                String.valueOf(this.getResultSize()));
        propertiesComponent.setValue(RefreshAction.TOP_COUNT,
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
                RefreshAction.LINES_FROM_CURSOR,
                String.valueOf(RefreshAction.LINES_FROM_CURSOR_DEFAULT_VALUE)));
        int persistedResultSize = Integer.parseInt(propertiesComponent.getValue(
                RefreshAction.SIZE,
                String.valueOf(RefreshAction.SIZE_DEFAULT_VALUE)));
        int persistedTopCount = Integer.parseInt(propertiesComponent.getValue(
                RefreshAction.TOP_COUNT,
                String.valueOf(RefreshAction.TOP_COUNT_DEFAULT_VALUE)));
        this.setLinesFromCursor(getValueBasedOnLimits(persistedLinesFromCursor,
                LimitsPanel.LINES_FROM_CURSOR_MIN, LimitsPanel.LINES_FROM_CURSOR_MAX,
                RefreshAction.LINES_FROM_CURSOR_DEFAULT_VALUE));
        this.setResultSize(getValueBasedOnLimits(persistedResultSize, LimitsPanel.MIN,
                LimitsPanel.RESULT_SIZE_MAX, RefreshAction.SIZE_DEFAULT_VALUE));
        this.setTopCount(getValueBasedOnLimits(persistedTopCount, LimitsPanel.MIN,
                LimitsPanel.TOP_COUNT_MAX, RefreshAction.TOP_COUNT_DEFAULT_VALUE));
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
