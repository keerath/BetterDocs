package com.imaginea.kodebeagle.model;

import com.imaginea.kodebeagle.action.RefreshAction;
import com.intellij.ide.util.PropertiesComponent;
import java.util.Arrays;

public class ElasticSearch {

    private static final int PRIME = 31;
    private String selectedEsURL;
    private String[] esURLS;
    private boolean esOverrideCheckBoxValue;
    private final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

    public ElasticSearch(final String pSelectedEsURL, final boolean pEsOverrideCheckBoxValue) {
        this.selectedEsURL = pSelectedEsURL;
        this.esOverrideCheckBoxValue = pEsOverrideCheckBoxValue;
        if (propertiesComponent.isValueSet(RefreshAction.ES_URL_VALUES)) {
            esURLS = propertiesComponent.getValues(RefreshAction.ES_URL_VALUES);
        }
    }

    public ElasticSearch() {
        retrieve();
    }

    public final String getSelectedEsURL() {
        return selectedEsURL;
    }

    public final String[] getEsURLS() {

        if (esURLS == null) {
            esURLS = new String[]{};
        }
        return esURLS.clone();
    }

    public final boolean getEsOverrideCheckBoxValue() {
        return esOverrideCheckBoxValue;
    }

    public final void setEsOverrideCheckBoxValue(final boolean pEsOverrideCheckBoxValue) {
        this.esOverrideCheckBoxValue = pEsOverrideCheckBoxValue;
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
        this.setEsOverrideCheckBoxValue(Boolean.valueOf(propertiesComponent.getValue(
                RefreshAction.ES_URL_CHECKBOX_VALUE,
                RefreshAction.ES_URL_DEFAULT_CHECKBOX_VALUE)));
        this.setSelectedEsURL(propertiesComponent.getValue(RefreshAction.ES_URL,
                RefreshAction.ES_URL_DEFAULT));
        this.setEsURLS(propertiesComponent.getValues(RefreshAction.ES_URL_VALUES));
    }

    public final void save() {

        propertiesComponent.setValue(RefreshAction.ES_URL_CHECKBOX_VALUE,
                String.valueOf(this.getEsOverrideCheckBoxValue()));
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
        ElasticSearch myElasticSearch = (ElasticSearch) obj;
        return this.getSelectedEsURL().equals(myElasticSearch.getSelectedEsURL());
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
