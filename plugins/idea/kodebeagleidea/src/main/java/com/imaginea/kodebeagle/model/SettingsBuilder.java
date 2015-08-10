package com.imaginea.kodebeagle.model;

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
