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
import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.intellij.ide.util.PropertiesComponent;
import java.util.UUID;

public class Identity {

    public static final String BEAGLE_ID = "Beagle ID:";
    private static final String NA = "Not Available";
    private static final int PRIME = 31;
    private String beagleIdValue;
    private boolean optOutCheckBoxValue;
    private WindowObjects windowObjects = WindowObjects.getInstance();
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
        propertiesComponent.setValue(RefreshActionBase.OPT_OUT_CHECKBOX_VALUE,
                String.valueOf(optOutCheckBoxValue));
    }

    private void retrieve() {
        this.optOutCheckBoxValue = Boolean.valueOf(
                propertiesComponent.getValue(RefreshActionBase.OPT_OUT_CHECKBOX_VALUE));
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
