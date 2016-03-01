package com.kodebeagle.hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public abstract class HdfsBaseUtil {
    private FileSystem fileSystem;

    public HdfsBaseUtil() {
        initFileSystem();
    }

    public void initFileSystem() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.145:9000/");
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }
}
