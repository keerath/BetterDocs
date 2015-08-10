package com.imaginea.kodebeagle.model;


import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.intellij.ide.util.PropertiesComponent;
import java.util.UUID;

public class Identity {

    public static final String BEAGLE_ID = "Beagle ID:";
    private static final String NA = "Not Available";
    private static final int PRIME = 31;
    private String beagleIdValue;
    private boolean optOutCheckBoxValue;
    private WindowObjects windowObjects = WindowObjects.getWindowObjects();
    private final PropertiesComponent propertiesComponent = PropertiesComponent.getInstance();

    public Identity(final boolean pOptOutCheckBoxValue) {
        this.optOutCheckBoxValue = pOptOutCheckBoxValue;
    }

    public Identity() {
        retrieve();
    }

    public final String getBeagleIdValue() {
        return beagleIdValue;
    }

    public final boolean getOptOutCheckBoxValue() {
        return optOutCheckBoxValue;
    }

    public final void loadBeagleId() {
        if (!optOutCheckBoxValue) {
            if (!propertiesComponent.isValueSet(BEAGLE_ID)) {
                windowObjects.setBeagleId(UUID.randomUUID().toString());
                beagleIdValue = windowObjects.getBeagleId();
            } else {
                beagleIdValue = propertiesComponent.getValue(BEAGLE_ID);
            }
        } else {
            beagleIdValue = NA;
        }
    }

    public final void save() {
        propertiesComponent.setValue(RefreshAction.OPT_OUT_CHECKBOX_VALUE,
                String.valueOf(optOutCheckBoxValue));
    }

    private void retrieve() {
        this.optOutCheckBoxValue = Boolean.valueOf(
                propertiesComponent.getValue(RefreshAction.OPT_OUT_CHECKBOX_VALUE));
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        Identity identity = (Identity) obj;
        return this.getOptOutCheckBoxValue() == identity.getOptOutCheckBoxValue();
    }

    @Override
    public final int hashCode() {
        return  PRIME * Boolean.valueOf(optOutCheckBoxValue).hashCode();
    }
}
