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

import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.ui.LimitsPanel;
import com.intellij.debugger.impl.DebuggerUtilsEx;
import com.intellij.ide.util.PropertiesComponent;
import com.intellij.ui.classFilter.ClassFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Settings {
    private static final int PRIME = 31;
    private static final String DEFAULT_PATTERN_1 = "org.sl4j.Logger";
    private static final String DEFAULT_PATTERN_2 = "java.util.[A-Z][a-z0-9]*";
    private static final String DELIMITER = ",";
    private static final String TRUE = "true";
    private Limits limits;
    private EsURLComboBoxModel esURLComboBoxModel;
    private boolean esOverrideCheckBoxValue;
    private List<ClassFilter> filterList;
    private boolean excludeImportsCheckBoxValue;
    private boolean optOutCheckBoxValue;



    public Settings(final Limits pLimits, final EsURLComboBoxModel pEsURLComboBoxModel,
                    final boolean pEsOverrideCheckBoxValue,
                    final List<ClassFilter> pFilterList,
                    final boolean pExcludeImportsCheckBoxValue,
                    final boolean pOptOutCheckBoxValue) {
        this.limits = pLimits;
        this.esURLComboBoxModel = pEsURLComboBoxModel;
        this.esOverrideCheckBoxValue = pEsOverrideCheckBoxValue;
        this.filterList = pFilterList;
        this.excludeImportsCheckBoxValue = pExcludeImportsCheckBoxValue;
        this.optOutCheckBoxValue = pOptOutCheckBoxValue;
    }

    public final Limits getLimits() {
        return limits;
    }

    public final void setLimits(final Limits pLimits) {
        this.limits = pLimits;
    }

    public final EsURLComboBoxModel getEsURLComboBoxModel() {
        return esURLComboBoxModel;
    }

    public final void setEsURLComboBoxModel(final EsURLComboBoxModel pEsURLComboBoxModel) {
        this.esURLComboBoxModel = pEsURLComboBoxModel;
    }

    public final boolean getOptOutCheckBoxValue() {
        return optOutCheckBoxValue;
    }

    public final void setOptOutCheckBoxValue(final boolean pOptOutCheckBoxValue) {
        this.optOutCheckBoxValue = pOptOutCheckBoxValue;
    }

    public static class Limits {
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

        private void save() {
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

    public static class EsURLComboBoxModel {
        private String selectedEsURL;
        private String[] esURLS;
        private final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

        public EsURLComboBoxModel(final String pSelectedEsURL) {
            this.selectedEsURL = pSelectedEsURL;
            if (propertiesComponent.isValueSet(RefreshAction.ES_URL_VALUES)) {
                esURLS = propertiesComponent.getValues(RefreshAction.ES_URL_VALUES);
            }
        }

        public EsURLComboBoxModel() {
            retrieve();
        }

        public final String getSelectedEsURL() {
            return  selectedEsURL;
        }

        public final String[] getEsURLS() {

            if (esURLS == null) {
                esURLS = new String[]{};
            }
            return esURLS.clone();
        }

        public final void setSelectedEsURL(final String pSelectedEsURL) {
            this.selectedEsURL = pSelectedEsURL;
        }

        public final void setEsURLS(final String[] pEsURLS) {
            if (pEsURLS != null) {
                esURLS = new String[pEsURLS.length];
                System.arraycopy(pEsURLS, 0, esURLS, 0, pEsURLS.length);
            }
        }

        private void retrieve() {
            this.setSelectedEsURL(propertiesComponent.getValue(RefreshAction.ES_URL,
                    RefreshAction.ES_URL_DEFAULT));
            this.setEsURLS(propertiesComponent.getValues(RefreshAction.ES_URL_VALUES));
        }

        private void save() {
            propertiesComponent.setValue(RefreshAction.ES_URL, this.getSelectedEsURL());
            propertiesComponent.setValues(RefreshAction.ES_URL_VALUES, this.getEsURLS());
        }

        @Override
        public final boolean equals(final Object obj) {
            if (obj == this) {
                return  true;
            }
            if (obj == null || this.getClass() != obj.getClass()) {
                return false;
            }
            EsURLComboBoxModel myEsURLComboBoxModel = (EsURLComboBoxModel) obj;
            return this.getSelectedEsURL().equals(myEsURLComboBoxModel.getSelectedEsURL());
        }

        @Override
        public final int hashCode() {
            int hashCode = 0;
            if (selectedEsURL != null) {
                hashCode = PRIME * selectedEsURL.hashCode() + hashCode;
            }
            if (esURLS != null) {
                hashCode = PRIME * Arrays.hashCode(esURLS) + hashCode;
            }
            return hashCode;
        }
    }

    public Settings() {
        retrieve();
    }

    public final boolean getEsOverrideCheckBoxValue() {
        return esOverrideCheckBoxValue;
    }

    public final boolean getExcludeImportsCheckBoxValue() {
        return excludeImportsCheckBoxValue;
    }

    public final List<ClassFilter> getFilterList() {
        if (filterList != null) {
            return filterList;
        } else {
            return new ArrayList<>();
        }
    }

    public final List<ClassFilter> getClassFiltersFromPatternAndState(
            final String[] pExcludeImportPatterns, final String[] pExcludeImportStates) {

        List<ClassFilter> filtersList = new ArrayList<>();
        List<String> excludeImportPatterns = Arrays.asList(pExcludeImportPatterns);
        List<String> excludeImportStates = Arrays.asList(pExcludeImportStates);
        if (excludeImportPatterns.size() == excludeImportStates.size()
                && !containsOnlyEmptyString(excludeImportPatterns)
                && !containsOnlyEmptyString(excludeImportStates)) {
            for (int i = 0; i < excludeImportPatterns.size(); i++) {
                String pattern = excludeImportPatterns.get(i);
                boolean state = Boolean.valueOf(excludeImportStates.get(i));
                ClassFilter classFilter = new ClassFilter(pattern);
                classFilter.setEnabled(state);
                filtersList.add(classFilter);
            }
        }
        return filtersList;
    }

    private boolean containsOnlyEmptyString(final List<String> pList) {
        return !pList.isEmpty() && pList.get(0).equals("");
    }

    private String[] getImportPatterns(final Iterable<ClassFilter> filters) {
        List<String> importsPatterns = new ArrayList<>();
        for (ClassFilter filter : filters) {
            importsPatterns.add(filter.getPattern());
        }
        return importsPatterns.toArray(new String[importsPatterns.size()]);
    }

    private String[] getImportStates(final Iterable<ClassFilter> filters) {
        List<String> importStates = new ArrayList<>();
        for (ClassFilter filter : filters) {
            importStates.add(String.valueOf(filter.isEnabled()));
        }
        return importStates.toArray(new String[importStates.size()]);
    }

    public final void setEsOverrideCheckBoxValue(final boolean pEsOverrideCheckBoxValue) {
        this.esOverrideCheckBoxValue = pEsOverrideCheckBoxValue;
    }

    public final void setExcludeImportsCheckBoxValue(final boolean pExcludeImportsCheckBoxValue) {
        this.excludeImportsCheckBoxValue = pExcludeImportsCheckBoxValue;
    }

    public final void setFilterList(final List<ClassFilter> pFilterList) {
        this.filterList = pFilterList;
    }

    public final void save() {
        final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        Limits myLimits = getLimits();
        myLimits.save();
        EsURLComboBoxModel myEsURLComboBoxModel = getEsURLComboBoxModel();
        myEsURLComboBoxModel.save();
        propertiesComponent.setValue(RefreshAction.ES_URL_CHECKBOX_VALUE,
                String.valueOf(esOverrideCheckBoxValue));
        propertiesComponent.setValues(RefreshAction.EXCLUDE_IMPORT_PATTERN,
                getImportPatterns(getFilterList()));
        propertiesComponent.setValues(RefreshAction.EXCLUDE_IMPORT_STATE,
                getImportStates(getFilterList()));
        propertiesComponent.setValue(RefreshAction.EXCLUDE_IMPORT_CHECKBOX_VALUE,
                String.valueOf(excludeImportsCheckBoxValue));
        propertiesComponent.setValue(RefreshAction.OPT_OUT_CHECKBOX_VALUE,
                String.valueOf(optOutCheckBoxValue));
    }

    private void setOldImportsAsPatternAndState(final String oldImports) {
        PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        List<String> oldPatterns = new ArrayList<>();
        List<String> oldStates = new ArrayList<>();
        if (oldImports != null) {
            List<String> oldImportsList = Arrays.asList(oldImports.split(DELIMITER));
            for (String oldImport : oldImportsList) {
                String trimmedImport = oldImport.trim();
                if (!trimmedImport.isEmpty()) {
                    oldPatterns.add(trimmedImport);
                    oldStates.add(TRUE);
                }
            }
        }
        propertiesComponent.setValues(RefreshAction.EXCLUDE_IMPORT_PATTERN,
                oldPatterns.toArray(new String[oldPatterns.size()]));
        propertiesComponent.setValues(RefreshAction.EXCLUDE_IMPORT_STATE,
                oldStates.toArray(new String[oldStates.size()]));
    }

    private void retrieve() {
        final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();
        Limits myLimits = new Limits();
        this.setLimits(myLimits);
        EsURLComboBoxModel myEsURLComboBoxModel = new EsURLComboBoxModel();
        this.setEsURLComboBoxModel(myEsURLComboBoxModel);
        this.setEsOverrideCheckBoxValue(Boolean.parseBoolean(propertiesComponent.getValue(
                RefreshAction.ES_URL_CHECKBOX_VALUE,
                RefreshAction.ES_URL_DEFAULT_CHECKBOX_VALUE)));
        if (propertiesComponent.isValueSet(RefreshAction.OLD_EXCLUDE_IMPORT_LIST)) {
            setOldImportsAsPatternAndState(propertiesComponent.getValue(
                    RefreshAction.OLD_EXCLUDE_IMPORT_LIST));
            propertiesComponent.unsetValue(RefreshAction.OLD_EXCLUDE_IMPORT_LIST);
        }
        if (propertiesComponent.isValueSet(RefreshAction.EXCLUDE_IMPORT_PATTERN)
                && propertiesComponent.isValueSet(RefreshAction.EXCLUDE_IMPORT_STATE)) {
            String[] excludeImportStates = propertiesComponent.getValues(
                    RefreshAction.EXCLUDE_IMPORT_STATE);
            String[] excludeImportPatterns = propertiesComponent.getValues(
                    RefreshAction.EXCLUDE_IMPORT_PATTERN);
            if (excludeImportPatterns != null && excludeImportStates != null) {
                List<ClassFilter> filters =
                        getClassFiltersFromPatternAndState(excludeImportPatterns,
                                excludeImportStates);
                this.setFilterList(filters);
            }
        } else {
            List<ClassFilter> filters = new ArrayList<>();
            ClassFilter filter1 = new ClassFilter(DEFAULT_PATTERN_1);
            ClassFilter filter2 = new ClassFilter(DEFAULT_PATTERN_2);
            filter1.setEnabled(true);
            filter2.setEnabled(true);
            filters.add(filter1);
            filters.add(filter2);
            this.setFilterList(filters);
        }
        this.setExcludeImportsCheckBoxValue(Boolean.parseBoolean(propertiesComponent.getValue(
                RefreshAction.EXCLUDE_IMPORT_CHECKBOX_VALUE,
                RefreshAction.EXCLUDE_IMPORT_DEFAULT_CHECKBOX_VALUE)));

        this.setOptOutCheckBoxValue(Boolean.parseBoolean(propertiesComponent.getValue(
                RefreshAction.OPT_OUT_CHECKBOX_VALUE,
                RefreshAction.OPT_OUT_DEFAULT_CHECKBOX_VALUE)));
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        Settings settings = (Settings) obj;
        return  this.getLimits().equals(settings.getLimits())
                && getExcludeImportsCheckBoxValue() == settings.getExcludeImportsCheckBoxValue()
                && DebuggerUtilsEx.filterEquals(getFilterList().toArray(
                        new ClassFilter[getFilterList().size()]),
                settings.getFilterList().toArray(
                        new ClassFilter[settings.getFilterList().size()]))
                && getEsOverrideCheckBoxValue() == settings.getEsOverrideCheckBoxValue()
                && this.getEsURLComboBoxModel().equals(settings.getEsURLComboBoxModel())
                && this.getOptOutCheckBoxValue() == settings.getOptOutCheckBoxValue();
    }

    @Override
    public final int hashCode() {
        int hashCode = 0;
        if (limits != null) {
            hashCode = PRIME * limits.hashCode() + hashCode;
        }
        if (esURLComboBoxModel != null) {
            hashCode = PRIME * esURLComboBoxModel.hashCode() + hashCode;
        }
        if (filterList != null) {
            hashCode = PRIME * filterList.hashCode() + hashCode;
        }
        hashCode = String.valueOf(esOverrideCheckBoxValue).hashCode() + hashCode;
        hashCode = String.valueOf(excludeImportsCheckBoxValue).hashCode() + hashCode;
        hashCode = String.valueOf(optOutCheckBoxValue).hashCode() + hashCode;
        return hashCode;
    }
}

