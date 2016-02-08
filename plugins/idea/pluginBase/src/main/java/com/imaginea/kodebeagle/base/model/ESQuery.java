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

import java.util.List;

@SuppressWarnings("PMD")
public class ESQuery {
    private Query query;
    private int from;
    private int size;
    private List<Sort> sort;

    public final Query getQuery() {
        return query;
    }

    public final int getFrom() {
        return from;
    }

    public final int getSize() {
        return size;
    }

    public final List<Sort> getSort() {
        return sort;
    }

    public static class Query {
        private Filtered filtered;
        private Bool bool;

        public final void setFiltered(final Filtered pFiltered) {
            this.filtered = pFiltered;
        }

        public final Filtered getFiltered() {
            return filtered;
        }

        public final Bool getBool() {
            return bool;
        }

        public final void setBool(final Bool pBool) {
            this.bool = pBool;
        }
    }

    public static class Filtered {
        private Query query;
        private Filter filter;
        private boolean cache;

        public final Filter getFilter() {
            return filter;
        }

        public final void setFilter(final Filter pFilter) {
            this.filter = pFilter;
        }

        public final boolean isCache() {
            return cache;
        }

        public final void setCache(final boolean pCache) {
            this.cache = pCache;
        }

        public final Query getQuery() {
            return query;
        }

        public final void setQuery(final Query pQuery) {
            this.query = pQuery;
        }
    }

    public static class Filter {
        private List<And> and;
        private Bool bool;

        public final Bool getBool() {
            return bool;
        }

        public final void setBool(final Bool pBool) {
            this.bool = pBool;
        }

        public final List<And> getAnd() {
            return and;
        }

        public final void setAnd(final List<And> pAnd) {
            this.and = pAnd;
        }
    }

    public static class And {
        private Nested nested;
        private Term term;

        public final Term getTerm() {
            return term;
        }

        public final void setTerm(final Term pTerm) {
            this.term = pTerm;
        }

        public final Nested getNested() {
            return nested;
        }

        public final void setNested(final Nested pNested) {
            this.nested = pNested;
        }
    }

    public static class Nested {
        private String path;
        private Filter filter;

        public final String getPath() {
            return path;
        }

        public final void setPath(final String pPath) {
            this.path = pPath;
        }

        public final Filter getFilter() {
            return filter;
        }

        public final void setFilter(final Filter pFilter) {
            this.filter = pFilter;
        }
    }

    public static class Bool {
        private List<Must> must;
        private List<Should> should;

        public final void setMust(final List<Must> pMust) {
            this.must = pMust;
        }

        public final void setShould(final List<Should> pShould) {
            this.should = pShould;
        }

        public final List<Must> getMust() {
            return must;
        }

        public final List<Should> getShould() {
            return should;
        }
    }

    public static class Must {
        private Term term;

        public final void setTerm(final Term pTerm) {
            this.term = pTerm;
        }

        public final Term getTerm() {
            return term;
        }
    }

    public static class Should {
        private Terms terms;

        public final Terms getTerms() {
            return terms;
        }

        public final void setTerms(final Terms pTerms) {
            this.terms = pTerms;
        }
    }

    public static class Term {
        private String importName;
        private String language;

        public final void setImportName(final String pImportName) {
            this.importName = pImportName;
        }

        public final String getImportName() {
            return importName;
        }

        public final String getLanguage() {
            return language;
        }

        public final void setLanguage(final String pLanguage) {
            this.language = pLanguage;
        }
    }

    public static class Terms {
        private List<String> methodName;

        public final List<String> getMethodName() {
            return methodName;
        }

        public final void setMethodName(final List<String> pMethodName) {
            this.methodName = pMethodName;
        }
    }

    public static class Sort {
        private Score score;

        public final void setScore(final Score pScore) {
            this.score = pScore;
        }

        public final Score getScore() {
            return score;
        }
    }

    public static class Score {
        private String order;

        public final void setOrder(final String pOrder) {
            this.order = pOrder;
        }

        public final String getOrder() {
            return order;
        }
    }

    public final void setQuery(final Query pQuery) {
        this.query = pQuery;
    }

    public final void setFrom(final int pFrom) {
        this.from = pFrom;
    }

    public final void setSize(final int pSize) {
        this.size = pSize;
    }

    public final void setSort(final List<Sort> pSort) {
        this.sort = pSort;
    }
}
