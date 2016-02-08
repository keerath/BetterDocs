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

import com.imaginea.kodebeagle.base.model.Settings;
import com.imaginea.kodebeagle.base.object.WindowObjects;
import com.imaginea.kodebeagle.base.ui.KBNotification;
import com.intellij.openapi.editor.Editor;
import com.intellij.openapi.roots.PackageIndex;
import com.intellij.openapi.util.Pair;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.ui.classFilter.ClassFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.jetbrains.annotations.NotNull;

public abstract class ImportsUtilBase {

    private static final String DOT = ".";

    public abstract Map<String, Set<String>> getImportInLines(final Editor projectEditor,
                                                              final Pair<Integer, Integer> pair);

    protected final Set<String> getExcludeImports() {
        Settings currentSettings = new Settings();
        Set<String> excludeImports = new HashSet<>();
        if (currentSettings.getImports().getExcludeImportsCheckBoxValue()) {
            List<ClassFilter> importFilters = currentSettings.getImports().getFilterList();
            for (ClassFilter importFilter : importFilters) {
                if (importFilter.isEnabled()) {
                    excludeImports.add(importFilter.getPattern());
                }
            }
        }
        return excludeImports;
    }

    public final Map<String, Set<String>> getFinalImports(
            final Editor projectEditor, final Pair<Integer, Integer> pair) {
        Map<String, Set<String>> importVsMethods =
                getImportInLines(projectEditor, pair);
        if (!importVsMethods.isEmpty()) {
            Set<String> excludeImports = getExcludeImports();
            if (!excludeImports.isEmpty()) {
                importVsMethods =
                        excludeConfiguredImports(importVsMethods, excludeImports);
            }
            Map<String, Set<String>> finalImports =
                    excludeInternalImports(importVsMethods);
            return finalImports;
        }
        return importVsMethods;
    }

    public final Map<String, Set<String>> excludeInternalImports(
            @NotNull final Map<String, Set<String>> importVsMethods) {
        final Map<String, Set<String>> importsAfterExclusion = new HashMap<>();
        Set<Map.Entry<String, Set<String>>> entrySet = importVsMethods.entrySet();
        Iterator<Map.Entry<String, Set<String>>> myIterator = entrySet.iterator();
        PackageIndex packageIndex =
                PackageIndex.getInstance(WindowObjects.getInstance().getProject());
        while (myIterator.hasNext()) {
            Map.Entry<String, Set<String>> entry = myIterator.next();
            int indexOfDot = entry.getKey().lastIndexOf(DOT);
            String packageName;
            if (indexOfDot != -1) {
                packageName = entry.getKey().substring(0, entry.getKey().lastIndexOf(DOT));
                List<VirtualFile> packageDirectories = Arrays.asList(
                        packageIndex.getDirectoriesByPackageName(packageName, false));
                for (VirtualFile packageDirectory : packageDirectories) {
                    if (!packageDirectory.isInLocalFileSystem()) {
                        importsAfterExclusion.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        return importsAfterExclusion;
    }

    public final Map<String, Set<String>> excludeConfiguredImports(
            final Map<String, Set<String>> importsVsMethods, final Set<String> excludeImports) {
        Map<String, Set<String>> finalImportsVsMethods = new HashMap<>();
        finalImportsVsMethods.putAll(importsVsMethods);
        Set<Map.Entry<String, Set<String>>> entrySet = importsVsMethods.entrySet();
        for (String importStatement : excludeImports) {
            Pattern pattern = Pattern.compile(importStatement);
            for (Map.Entry<String, Set<String>> entry : entrySet) {
                try {
                    String entryImport = entry.getKey();
                    Matcher matcher = pattern.matcher(entryImport);
                    if (matcher.find()) {
                        finalImportsVsMethods.remove(entryImport);
                    }
                } catch (PatternSyntaxException e) {
                    KBNotification.getInstance().error(e);
                    e.printStackTrace();
                }
            }
        }
        return finalImportsVsMethods;
    }
}
