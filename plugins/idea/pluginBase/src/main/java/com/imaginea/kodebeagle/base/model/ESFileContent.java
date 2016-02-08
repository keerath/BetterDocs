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

public class ESFileContent {
    private Query query;

    public static class Query {
        private Bool bool;
        private Term term;

        public final void setBool(final Bool pBool) {
            this.bool = pBool;
        }

        public final Bool getBool() {
            return bool;
        }

        public final void setTerm(final Term pTerm) {
            this.term = pTerm;
        }

        public final Term getTerm() {
            return term;
        }
    }

    public static class  Bool {
        private List<Should> should;

        public final List<Should> getShould() {
            return should;
        }

        public final void setShould(final List<Should> pShould) {
            this.should = pShould;
        }
    }

    public static class Should {
        private Term term;

        public final void setTerm(final Term pTerm) {
            this.term = pTerm;
        }

        public final Term getTerm() {
            return term;
        }
    }

    public static class Term {
        private String fileName;

        public final void setFileName(final String pFileName) {
            this.fileName = pFileName;
        }

        public final String getFileName() {
            return fileName;
        }
    }

    public final Query getQuery() {
        return query;
    }

    public final void setQuery(final Query pQuery) {
        this.query = pQuery;
    }
}
