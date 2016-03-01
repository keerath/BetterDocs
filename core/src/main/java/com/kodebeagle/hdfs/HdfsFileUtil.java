package com.kodebeagle.hdfs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsFileUtil extends HdfsBaseUtil {
    private FileSystem fileSystem;

    public HdfsFileUtil() {
        super();
        this.fileSystem = getFileSystem();
    }

    public List<LocatedFileStatus> listFiles(String path, boolean recursive) {
        try {
            return toList(fileSystem.listFiles(new Path(path), recursive));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private <T> List<T> toList(RemoteIterator<T> remoteIterator) throws IOException {
        List<T> myList = new LinkedList<>();
        while (remoteIterator.hasNext()) {
            myList.add(remoteIterator.next());
        }
        return myList;
    }
}
