package com.imaginea.kodebeagle.model;

import com.imaginea.kodebeagle.action.RefreshAction;
import com.intellij.ide.util.PropertiesComponent;
import com.intellij.ui.classFilter.ClassFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Imports {

    private static final int PRIME = 31;
    private static final String DELIMITER = ",";
    private static final String TRUE = "true";
    private static final String DEFAULT_PATTERN_1 = "org.sl4j.Logger";
    private static final String DEFAULT_PATTERN_2 = "java.util.[A-Z][a-z0-9]*";
    private List<ClassFilter> filterList;
    private boolean excludeImportsCheckBoxValue;
    private final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

    public final List<ClassFilter> getFilterList() {
        return filterList;
    }

    public final void setFilterList(final List<ClassFilter> pFilterList) {
        this.filterList = pFilterList;
    }

    public final boolean getExcludeImportsCheckBoxValue() {
        return excludeImportsCheckBoxValue;
    }

    public final void setExcludeImportsCheckBoxValue(final boolean pExcludeImportsCheckBoxValue) {
        this.excludeImportsCheckBoxValue = pExcludeImportsCheckBoxValue;
    }

    public Imports(final List<ClassFilter> pFilterList,
                   final boolean pExcludeImportsCheckBoxValue) {
        this.filterList = pFilterList;
        this.excludeImportsCheckBoxValue = pExcludeImportsCheckBoxValue;
    }

    public Imports() {
        retrieve();
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

    private void setOldImportsAsPatternAndState(final String oldImports) {
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

    public final void save() {
        propertiesComponent.setValues(RefreshAction.EXCLUDE_IMPORT_PATTERN,
                getImportPatterns(getFilterList()));
        propertiesComponent.setValues(RefreshAction.EXCLUDE_IMPORT_STATE,
                getImportStates(getFilterList()));
        propertiesComponent.setValue(RefreshAction.EXCLUDE_IMPORT_CHECKBOX_VALUE,
                String.valueOf(excludeImportsCheckBoxValue));
    }


    private void retrieve() {
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
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        Imports imports = (Imports) obj;
        return this.getFilterList().equals(imports.getFilterList())
                && this.getExcludeImportsCheckBoxValue()
                == imports.getExcludeImportsCheckBoxValue();
    }

    @Override
    public final int hashCode() {
        int hashCode = 0;
        hashCode = PRIME * Boolean.valueOf(excludeImportsCheckBoxValue).hashCode() + hashCode;
        hashCode = PRIME * filterList.hashCode() + hashCode;
        return hashCode;
    }
}
