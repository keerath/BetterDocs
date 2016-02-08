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

public class SettingsBuilder {

    private Identity identity;
    private Limits limits;
    private Imports imports;
    private ElasticSearch elasticSearch;
    private Notifications notifications;

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

    public final SettingsBuilder withIdentity(final Identity pIdentity) {
        this.identity = pIdentity;
        return this;
    }

    public final SettingsBuilder withLimits(final Limits pLimits) {
        this.limits = pLimits;
        return this;
    }

    public final SettingsBuilder withImports(final Imports pImports) {
        this.imports = pImports;
        return this;
    }

    public final SettingsBuilder withElasticSearch(final ElasticSearch pElasticSearch) {
        this.elasticSearch = pElasticSearch;
        return this;
    }

    public final SettingsBuilder withNotifications(final Notifications pNotifications) {
        this.notifications = pNotifications;
        return this;
    }

    public static final SettingsBuilder settings() {
        return new SettingsBuilder();
    }

    public final Settings build() {
        return new Settings(this);
    }
}
