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

package com.imaginea.kodebeagle.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.imaginea.kodebeagle.action.RefreshAction;
import com.imaginea.kodebeagle.model.Settings;
import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.ui.KBNotification;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class ESUtils {
    private static final String FILE_CONTENT = "fileContent";
    private static final String HITS = "hits";
    private static final String TOTAL_COUNT = "total";
    private static final String SOURCE = "_source";
    private static final String FILE = "file";
    private static final String TOKENS = "tokens";
    private static final String SOURCEFILE_SEARCH = "/sourcefile/_search?source=";
    private static final String REPOSITORY_SEARCH = "/repository/_search?source=";
    private static final String FAILED_HTTP_ERROR = "Connection Error: ";
    private static final String USER_AGENT = "USER-AGENT";
    private static final int HTTP_OK_STATUS = 200;
    private static final String REPO_ID = "repoId";
    private static final String STARGAZERS_COUNT = "stargazersCount";
    private static final String FILE_NAME = "fileName";
    private static final String UID = "&uid=";
    private static final String OPTED_OUT = "opted-out";

    private static WindowObjects windowObjects = WindowObjects.getInstance();
    private JSONUtils jsonUtils = new JSONUtils();
    private int resultCount;
    private long totalHitsCount;

    public final long getTotalHitsCount() {
        return totalHitsCount;
    }

    public final int getResultCount() {
        return resultCount;
    }

    public final void fetchContentsAndUpdateMap(final List<String> fileNames) {
        String esFileQueryJson = jsonUtils.getJsonForFileContent(fileNames);
        String esFileResultJson;
        esFileResultJson = getESResultJson(esFileQueryJson,
                windowObjects.getEsURL() + SOURCEFILE_SEARCH);
        JsonArray hitsArray = getJsonHitsArray(getJsonElements(esFileResultJson));

        for (JsonElement hits : hitsArray) {
            JsonObject hitObject = hits.getAsJsonObject();
            JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
            //Replacing \r as it's treated as bad end of line character
            String fileContent = sourceObject.getAsJsonPrimitive(FILE_CONTENT).
                    getAsString().replaceAll("\r", "");
            String fileName = sourceObject.getAsJsonPrimitive(FILE_NAME).getAsString();
            windowObjects.getFileNameContentsMap().put(fileName, fileContent);
        }
    }

    public final String getContentsForFile(final String fileName) {
        Map<String, String> fileNameContentsMap =
                windowObjects.getFileNameContentsMap();

        if (!fileNameContentsMap.containsKey(fileName)) {
            fetchContentsAndUpdateMap(Arrays.asList(fileName));
        }
        String fileContent = fileNameContentsMap.get(fileName);

        return fileContent;
    }

    public final Map<String, String> getFileTokens(final String esResultJson) {
        Map<String, String> fileTokenMap = new HashMap<String, String>();
        final JsonObject hitsObject = getJsonElements(esResultJson);
        JsonArray hitsArray = getJsonHitsArray(hitsObject);
        resultCount = hitsArray.size();
        totalHitsCount = getTotalHits(hitsObject);
        for (JsonElement hits : hitsArray) {
            JsonObject hitObject = hits.getAsJsonObject();
            JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
            String fileName = sourceObject.getAsJsonPrimitive(FILE).getAsString();
            //Extracting repoIds for future use
            int repoId = sourceObject.getAsJsonPrimitive(REPO_ID).getAsInt();
            String project = getProjectName(fileName);
            if (!windowObjects.getRepoNameIdMap().containsKey(project)) {
                windowObjects.getRepoNameIdMap().put(project, repoId);
            }

            String tokens = sourceObject.get(TOKENS).toString();
            fileTokenMap.put(fileName, tokens);
        }
        return fileTokenMap;
    }

    protected final JsonObject getJsonElements(final String esResultJson) {
        JsonReader reader = new JsonReader(new StringReader(esResultJson));
        reader.setLenient(true);
        JsonElement jsonElement = new JsonParser().parse(reader);
        return jsonElement.getAsJsonObject().getAsJsonObject(HITS);
    }

    private Long getTotalHits(final JsonObject hitsObject) {
        return hitsObject.get(TOTAL_COUNT).getAsLong();
    }

    private JsonArray getJsonHitsArray(final JsonObject hitsObject) {
        return hitsObject.getAsJsonArray(HITS);
    }


    public final String getESResultJson(final String esQueryJson, final String url) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            HttpClient httpClient = new DefaultHttpClient();
            String encodedJson = URLEncoder.encode(esQueryJson, StandardCharsets.UTF_8.name());
            StringBuilder esGetURL = new StringBuilder(url).append(encodedJson).append(UID);
            Settings currentSettings = new Settings();
            HttpGet getRequest;
            if (!currentSettings.getIdentity().getOptOutCheckBoxValue()) {
                esGetURL =
                        new StringBuilder(esGetURL).append(windowObjects.getBeagleId());
                String versionInfo = windowObjects.getOsInfo() + "  "
                        + windowObjects.getApplicationVersion() + "  "
                        + windowObjects.getPluginVersion();
                getRequest = new HttpGet(esGetURL.toString());
                getRequest.setHeader(USER_AGENT, versionInfo);
            } else {
                esGetURL = new StringBuilder(esGetURL).append(OPTED_OUT);
                getRequest = new HttpGet(esGetURL.toString());
            }
            HttpResponse response = httpClient.execute(getRequest);
            if (response.getStatusLine().getStatusCode() != HTTP_OK_STATUS) {
                throw new RuntimeException(FAILED_HTTP_ERROR
                        + response.getStatusLine().getStatusCode() + "  "
                        + response.getStatusLine().getReasonPhrase());
            }

            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent()),
                            StandardCharsets.UTF_8.name()));
            String output;
            while ((output = bufferedReader.readLine()) != null) {
                stringBuilder.append(output);
            }
            bufferedReader.close();
            httpClient.getConnectionManager().shutdown();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return handleHttpException(e);
        }
        return stringBuilder.toString();
    }

    private String handleHttpException(final Exception e) {
        KBNotification.getInstance().error(e);
        e.printStackTrace();
        return RefreshAction.EMPTY_ES_URL;
    }

    public final String getRepoStars(final String repoStarsJson) {
        String repoStarResultJson;
        String stars = null;
        repoStarResultJson = getESResultJson(repoStarsJson,
                windowObjects.getEsURL() + REPOSITORY_SEARCH);
        JsonArray hitsArray = getJsonHitsArray(getJsonElements(repoStarResultJson));

        JsonObject hitObject = hitsArray.get(0).getAsJsonObject();
        JsonObject sourceObject = hitObject.getAsJsonObject(SOURCE);
        //Replacing \r as it's treated as bad end of line character
        stars = sourceObject.getAsJsonPrimitive(STARGAZERS_COUNT).getAsString();
        return stars;
    }

    public final String getProjectName(final String fileName) {
        //Project name is till 2nd '/'
        int startIndex = fileName.indexOf('/');
        int endIndex = fileName.indexOf('/', startIndex + 1);
        return fileName.substring(0, endIndex);
    }

    public final String extractRepoStars(final String repoName, final int repoId) {
        String stars;
        if (windowObjects.getRepoStarsMap().containsKey(repoName)) {
            stars = windowObjects.getRepoStarsMap().get(repoName).toString();
        } else {
            String repoStarsJson = jsonUtils.getRepoStarsJSON(repoId);
            stars = getRepoStars(repoStarsJson);
            windowObjects.getRepoStarsMap().put(repoName, stars);
        }
        return stars;
    }

}
