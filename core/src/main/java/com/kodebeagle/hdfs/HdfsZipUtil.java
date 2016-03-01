package com.kodebeagle.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsZipUtil extends HdfsBaseUtil {

    private static final int BUFFER_SIZE = 4096;
    private FileSystem fs;

    public HdfsZipUtil() {
        super();
        fs = getFileSystem();
    }

    public void unzip(final String zipFilePath, final String destDirectory) throws IOException, InterruptedException {
        Path path = new Path(zipFilePath);
        fs.mkdirs(new Path(destDirectory));
        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fs.open(path)));
        ZipEntry entry = zis.getNextEntry();
        while (entry != null) {
            String filePath = destDirectory + File.separator + entry.getName();
            if (!entry.isDirectory()) {
                extractFile(zis, fs.create(new Path(destDirectory)));
            } else {
                File dir = new File(filePath);
                dir.mkdir();
            }
            zis.closeEntry();
            entry = zis.getNextEntry();
        }
        zis.close();
    }

    private void extractFile(ZipInputStream zis, OutputStream os) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(os);
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read;
        while ((read = zis.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
}
