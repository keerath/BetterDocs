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

import com.imaginea.kodebeagle.ui.KBNotification;
import com.intellij.openapi.util.io.FileUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Hex;
import org.jetbrains.annotations.NotNull;


public final class Utils {

    private static final Utils INSTANCE = new Utils();

    private Utils() {
        // Restrict others from creating an instance.
    }

    public static Utils getInstance() {
        return INSTANCE;
    }
    @NotNull
    public String createFileWithContents(final String displayFileName, final String contents,
                                         final String baseDir, final String digest)
            throws IOException {

        final String fileParentPath =
                String.format("%s%c%s", baseDir, File.separatorChar, digest);
        final File parentDir = new File(fileParentPath);
        FileUtil.createDirectory(parentDir);
        parentDir.deleteOnExit();
        final String fullFilePath =
                String.format("%s%c%s",
                        parentDir.getAbsolutePath(), File.separatorChar, displayFileName);
        final File file = new File(fullFilePath);
        if (!file.exists()) {
            FileUtil.createIfDoesntExist(file);
            forceWrite(contents, file);
            file.deleteOnExit();
        }
        return fullFilePath;
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
        StringBuilder legalNoticeMessage = new StringBuilder();
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(stream,
                             StandardCharsets.UTF_8.name()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                legalNoticeMessage.append(line);
            }
        } catch (IOException ioe) {
            KBNotification.getInstance().error(ioe);
            ioe.printStackTrace();
        }
        return legalNoticeMessage.toString();
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
