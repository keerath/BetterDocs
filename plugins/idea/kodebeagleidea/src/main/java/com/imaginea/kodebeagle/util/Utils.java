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

import com.imaginea.kodebeagle.object.WindowObjects;
import com.imaginea.kodebeagle.ui.KBNotification;
import com.intellij.openapi.util.io.FileUtil;
import com.intellij.openapi.util.text.StringUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Utils {

    private static final Utils INSTANCE = new Utils();
    private static final String JAVA_IO_TMP_DIR = "java.io.tmpdir";
    private static final String KBCACHE = "KBCache";
    public static final String TMP = ".tmp";
    public static final String JAVA = ".java";
    private WindowObjects windowObjects = WindowObjects.getInstance();


    private Utils() {
        // Restrict others from creating an instance.
    }

    public static Utils getInstance() {
        return INSTANCE;
    }

    public File getTempFile(final String absoluteFileName)  {
        File file = getFile(absoluteFileName);
        File tmpFile = null;
        if (file != null) {
            String fileName = file.getName();
            try {
                String finalName = fileName.substring(0, fileName.lastIndexOf('.'))
                        .concat(TMP).concat(JAVA);
                tmpFile = new File(file.getParentFile(), finalName);
                FileUtil.createIfDoesntExist(tmpFile);
                FileUtil.copy(file, tmpFile);
                tmpFile.deleteOnExit();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tmpFile;
    }

    public File getFile(final String absoluteFileName) {
        final String tempDir = System.getProperty(JAVA_IO_TMP_DIR);
        final String baseDir = String.format("%s%c%s", tempDir, File.separatorChar, KBCACHE);
        final String displayName = getDisplayFileName(absoluteFileName);
        if (!displayName.equals("")) {
            final String trimmedFileName =
                    FileUtil.sanitizeFileName(StringUtil.trimEnd(absoluteFileName, displayName));
            String digest = "";
            try {
                digest = getDigestAsString(trimmedFileName);
            } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
                KBNotification.getInstance().error(e);
                e.printStackTrace();
            }
            final String fileParentPath =
                    String.format("%s%c%s", baseDir, File.separatorChar, digest);
            final File parentDir = new File(fileParentPath);
            if (!parentDir.exists()) {
                FileUtil.createDirectory(parentDir);
            }
            final String fullFilePath =
                    String.format("%s%c%s",
                            parentDir.getAbsolutePath(), File.separatorChar, displayName);
            return new File(fullFilePath);
        }
        return null;
    }

    @Nullable
    public String createFileWithContents(final String absoluteFileName, final String fileContents) {
        File file = getFile(absoluteFileName);
        String absolutePath = null;
        if (file != null && !file.exists()) {
            FileUtil.createIfDoesntExist(file);
            try {
                forceWrite(fileContents, file);
            } catch (IOException e) {
                KBNotification.getInstance().error(e);
                e.printStackTrace();
            }
             absolutePath = file.getAbsolutePath();
        }
        return absolutePath;
    }

    private String getDisplayFileName(final String absoluteFileName) {
        int indexOfSlash = absoluteFileName.lastIndexOf('/');
        if (indexOfSlash != -1) {
            return absoluteFileName.substring(indexOfSlash);
        }
        return "";
    }

    public List<String> getFilesNotCached(final List<String> files) {
        List<String> filesNotCached = new ArrayList<>();
        for (String fileName : files) {
            if (!windowObjects.getFileNameContentsMap().containsKey(fileName)) {
                File file = Utils.getInstance().getFile(fileName);
                if (file != null && file.exists()) {
                    try {
                        String fileContent = Utils.getInstance().readStreamFully(
                                new FileInputStream(file));
                        windowObjects.getFileNameContentsMap().put(fileName, fileContent);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                } else if (file != null && !file.exists()) {
                    filesNotCached.add(fileName);
                }
            }
        }
        return filesNotCached;
    }

    public void forceWrite(final String contents, final File file) throws IOException {
        try (FileOutputStream s = new FileOutputStream(file.getAbsolutePath(), false)) {
            s.write(contents.getBytes(StandardCharsets.UTF_8.name()));
            FileChannel c = s.getChannel();
            c.force(true);
            s.getFD().sync();
            c.close();
            s.close();
        }
    }

    @NotNull
    public String readStreamFully(final InputStream stream) {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(stream,
                             StandardCharsets.UTF_8.name()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line);
                content.append("\n");
            }
        } catch (IOException e) {
            KBNotification.getInstance().error(e);
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return content.toString();
    }

    @NotNull
    public String getDigestAsString(final String trimmedFileName)
            throws UnsupportedEncodingException, NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(trimmedFileName.getBytes(StandardCharsets.UTF_8));
        // We think 10 chars is safe enough to rely on.
        return Hex.encodeHexString(crypt.digest()).substring(0, 10);
    }
}
