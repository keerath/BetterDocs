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

package com.imaginea.kodebeagle.base.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.imaginea.kodebeagle.base.model.ESFileContent;
import com.imaginea.kodebeagle.base.model.ESQuery;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JSONUtils {
    private static final String TOKENS_IMPORT_NAME = "tokens.importName";
    private static final String IMPORT_NAME = "importName";
    private static final String TOKENS = "tokens";
    private static final String TOKENS_METHOD_NAME = "tokens.methodAndLineNumbers.methodName";
    private static final String METHOD_NAME = "methodName";
    private static final String IMPORT_EXACT_NAME = "importExactName";
    private static final String LINE_NUMBERS = "lineNumbers";
    private static final String CACHE = "cache";
    private static final String FILTER_CACHE = "_cache";
    private static final String SORT_ORDER = "desc";
    private static final String TYPESOURCEFILENAME_FILENAME = "typesourcefile.fileName";
    private static final String FILE_NAME = "fileName";
    private static final String LINE_NUMBER = "lineNumber";

    public final String getJsonForFileContent(final List<String> fileNameList) {
        ESFileContent esFileContent = new ESFileContent();
        ESFileContent.Query query = new ESFileContent.Query();
        esFileContent.setQuery(query);
        ESFileContent.Bool bool = new ESFileContent.Bool();
        query.setBool(bool);
        List<ESFileContent.Should> shouldList = new ArrayList<ESFileContent.Should>();
        bool.setShould(shouldList);

        for (String fileName : fileNameList) {
            ESFileContent.Should should = new ESFileContent.Should();
            ESFileContent.Term term = new ESFileContent.Term();
            should.setTerm(term);
            term.setFileName(fileName);
            shouldList.add(should);
        }

        Gson gson = new Gson();
        return gson.toJson(esFileContent).replaceAll(FILE_NAME, TYPESOURCEFILENAME_FILENAME);
    }

    private ESQuery getFilteredQuery(final int size) {
        ESQuery esQuery = new ESQuery();
        ESQuery.Query query = new ESQuery.Query();
        esQuery.setQuery(query);
        esQuery.setFrom(0);
        esQuery.setSize(size);
        List<ESQuery.Sort> sortList = new ArrayList<>();
        ESQuery.Sort sort = new ESQuery.Sort();
        ESQuery.Score score = new ESQuery.Score();
        score.setOrder(SORT_ORDER);
        sort.setScore(score);
        sortList.add(sort);
        esQuery.setSort(sortList);
        ESQuery.Filtered filtered = new ESQuery.Filtered();
        filtered.setCache(true);
        query.setFiltered(filtered);
        ESQuery.Filter filter = new ESQuery.Filter();
        filtered.setFilter(filter);
        List<ESQuery.And> andList = new ArrayList<>();
        filter.setAnd(andList);
        return esQuery;
    }

    private ESQuery.And getAndTerm(final Map.Entry<String, Set<String>> entry,
                                   final boolean includeMethods) {
        ESQuery.And and = new ESQuery.And();
        if (includeMethods) {
            ESQuery.Nested nested = new ESQuery.Nested();
            ESQuery.Filter innerFilter = new ESQuery.Filter();
            ESQuery.Bool bool = new ESQuery.Bool();
            List<ESQuery.Must> mustList = getImportMustList(entry.getKey());
            bool.setMust(mustList);
            if (!entry.getValue().isEmpty()) {
                List<ESQuery.Should> shouldList = getMethodsShouldList(entry.getValue());
                bool.setShould(shouldList);
            }
            innerFilter.setBool(bool);
            nested.setFilter(innerFilter);
            nested.setPath(TOKENS);
            and.setNested(nested);
            return and;
        } else {
            ESQuery.Term term = getImportTerm(entry.getKey());
            and.setTerm(term);
            return and;
        }
    }

    private List<ESQuery.Must> getImportMustList(final String importName) {
        List<ESQuery.Must> mustList = new ArrayList<>();
        ESQuery.Must must = new ESQuery.Must();
        ESQuery.Term term = getImportTerm(importName);
        must.setTerm(term);
        mustList.add(must);
        return mustList;
    }

    private ESQuery.Term getImportTerm(final String importName) {
        ESQuery.Term term = new ESQuery.Term();
        term.setImportName(importName.toLowerCase());
        return term;
    }

    private List<ESQuery.Should> getMethodsShouldList(final Set<String> methods) {
        List<ESQuery.Should> shouldList = new ArrayList<>();
        ESQuery.Should should = new ESQuery.Should();
        ESQuery.Terms terms = new ESQuery.Terms();
        List<String> methodsList = new ArrayList<>(methods);
        terms.setMethodName(methodsList);
        should.setTerms(terms);
        shouldList.add(should);
        return shouldList;
    }

    public final String getESQueryJson(
            final Map<String, Set<String>> importsInLines,
            final int size, final boolean includeMethods) {
        ESQuery esQuery = getFilteredQuery(size);
        List<ESQuery.And> andList = esQuery.getQuery().getFiltered().getFilter().getAnd();
        Set<Map.Entry<String, Set<String>>> entrySet = importsInLines.entrySet();
        for (Map.Entry<String, Set<String>> entry : entrySet) {
            ESQuery.And and = getAndTerm(entry, includeMethods);
            andList.add(and);
        }
        Gson gson = new Gson();
        return gson.toJson(esQuery).replaceAll(IMPORT_NAME,
                TOKENS_IMPORT_NAME).replaceAll(METHOD_NAME,
                TOKENS_METHOD_NAME).replace(CACHE, FILTER_CACHE);
    }

    public final List<Integer> getLineNumbers(final Collection<String> imports,
                                              final String tokens) {
        List<Integer> lineNumbers = new ArrayList<Integer>();
        JsonReader reader = new JsonReader(new StringReader(tokens));
        reader.setLenient(true);
        JsonArray tokensArray = new JsonParser().parse(reader).getAsJsonArray();
        for (JsonElement token : tokensArray) {
            JsonObject jObject = token.getAsJsonObject();
            String importName = jObject.getAsJsonPrimitive(IMPORT_EXACT_NAME).getAsString();
            if (imports.contains(importName)) {
                JsonArray lineNumbersArray = jObject.getAsJsonArray(LINE_NUMBERS);
                for (JsonElement lineNumberInfo : lineNumbersArray) {
                    lineNumbers.add(lineNumberInfo.getAsJsonObject()
                            .getAsJsonPrimitive(LINE_NUMBER).getAsInt());
                }
            }
        }
        return lineNumbers;
    }
}
