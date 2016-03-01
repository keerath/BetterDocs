package com.kodebeagle.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class UnzipUtil {

    private static final int BUFFER_SIZE = 4096;
    private static final UnzipUtil unzipUtil = new UnzipUtil();

    public static UnzipUtil getInstance() {
        return unzipUtil;
    }

    private UnzipUtil() {

    }

    public void unzip(final String zipFilePath, final String destDirectory) throws IOException, InterruptedException {
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zipFilePath)));
        ZipEntry entry = zis.getNextEntry();
        while (entry != null) {
            String filePath = destDirectory + File.separator + entry.getName();
            if (!entry.isDirectory()) {
                extractFile(zis, filePath);
            } else {
                File dir = new File(filePath);
                dir.mkdir();
            }
            zis.closeEntry();
            entry = zis.getNextEntry();
        }
        zis.close();
    }

    private void extractFile(ZipInputStream zis, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read;
        while ((read = zis.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
}