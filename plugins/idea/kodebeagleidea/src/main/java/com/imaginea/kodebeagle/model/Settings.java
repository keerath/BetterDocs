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

public class Settings {

    private static final int PRIME = 31;
    private Identity identity;
    private Limits limits;
    private Imports imports;
    private ElasticSearch elasticSearch;
    private Notifications notifications;

    public Settings(final SettingsBuilder pSettingsBuilder) {
        if (pSettingsBuilder.getIdentity() != null) {
            identity = pSettingsBuilder.getIdentity();
        }
        if (pSettingsBuilder.getLimits() != null) {
            limits = pSettingsBuilder.getLimits();
        }
        if (pSettingsBuilder.getImports() != null) {
            imports = pSettingsBuilder.getImports();
        }
        if (pSettingsBuilder.getElasticSearch() != null) {
            elasticSearch = pSettingsBuilder.getElasticSearch();
        }
        if (pSettingsBuilder.getNotifications() != null) {
            notifications = pSettingsBuilder.getNotifications();
        }
    }


    public Settings() {
        identity = new Identity();
        limits = new Limits();
        imports = new Imports();
        elasticSearch = new ElasticSearch();
        notifications = new Notifications();
    }

    public final Identity getIdentity() {
        return identity;
    }

    public final Limits getLimits() {
        return limits;
    }

    public final Imports getImports() {
        return imports;
    }

    public final ElasticSearch getElasticSearch() {
        return elasticSearch;
    }

    public final Notifications getNotifications() {
        return notifications;
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
        return this.getIdentity().equals(settings.getIdentity())
                && this.getLimits().equals(settings.getLimits())
                && this.getImports().equals(settings.getImports())
                && this.getElasticSearch().equals(settings.getElasticSearch())
                && this.getNotifications().equals(settings.getNotifications());
    }

    @Override
    public final int hashCode() {
        int hashCode = 0;
        if (identity != null) {
            hashCode = PRIME * identity.hashCode() + hashCode;
        }
        if (limits != null) {
            hashCode = PRIME * limits.hashCode() + hashCode;
        }
        if (imports != null) {
            hashCode = PRIME * imports.hashCode() + hashCode;
        }
        if (elasticSearch != null) {
            hashCode = PRIME * elasticSearch.hashCode() + hashCode;
        }
        if (notifications != null) {
            hashCode = PRIME * notifications.hashCode() + hashCode;
        }
        return hashCode;
    }

    public final void save() {
        identity.save();
        limits.save();
        imports.save();
        elasticSearch.save();
        notifications.save();
    }
}
