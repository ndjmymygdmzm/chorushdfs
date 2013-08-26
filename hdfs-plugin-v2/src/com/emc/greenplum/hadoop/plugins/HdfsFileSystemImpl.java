package com.emc.greenplum.hadoop.plugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class HdfsFileSystemImpl extends HdfsFileSystemPlugin {
    private FileSystem fileSystem;

    @Override
    public void loadFileSystem(String host, String port, String username, boolean isHA, List<HdfsPair> parameters) {
        loadHadoopClassLoader();
        Configuration config = new Configuration();

        config.set("fs.defaultFS", buildHdfsPath(host, port, isHA));

        if (parameters != null && parameters.size() > 0) {
            for (HdfsPair pair : parameters) {
                config.set(pair.getKey(), pair.getValue());
            }
        }

        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // some magic to make file contents readable using the existing getContents implementation
        config.set("dfs.client.use.legacy.blockreader", "true");

        try {
            fileSystem = FileSystem.get(FileSystem.getDefaultUri(config), config, username);
        } catch (Exception e) {
        } finally {
            restoreOriginalClassLoader();
        }
    }

    @Override
    public void closeFileSystem() {
        try {
            fileSystem.close();
        } catch (IOException e) {
        }
        fileSystem = null;
    }

    @Override
    public List<HdfsEntity> list(String path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(path));
        List<HdfsEntity> entities = new ArrayList<HdfsEntity>();

        for (FileStatus fileStatus : fileStatuses) {
            HdfsEntity entity = new HdfsEntity();

            entity.setDirectory(fileStatus.isDirectory());
            entity.setPath(fileStatus.getPath().toUri().getPath());
            entity.setModifiedAt(new Date(fileStatus.getModificationTime()));
            entity.setSize(fileStatus.getLen());

            if (fileStatus.isDirectory()) {
                Integer length = 0;
                try {
                    FileStatus[] contents = fileSystem.listStatus(fileStatus.getPath());
                    length = contents.length;
                } catch (org.apache.hadoop.security.AccessControlException exception) {
                }
                entity.setContentCount(length);
            }

            entities.add(entity);
        }

        return entities;
    }

    @Override
    public boolean loadedSuccessfully() {
        if (fileSystem != null) {
            try {
                return fileSystem.exists(new Path("/"));
            } catch (IOException e) {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public List<String> getContent(String path, int lineCount) throws IOException {
        FileStatus[] files = fileSystem.globStatus(new Path(path));
        ArrayList<String> lines = new ArrayList<String>();

        if (files != null) {
            for (FileStatus file : files) {

                if (lines.size() >= lineCount) { break; }

                if (!file.isDirectory()) {

                    DataInputStream in = fileSystem.open(file.getPath());

                    BufferedReader dataReader = new BufferedReader(new InputStreamReader(in));

                    String line = dataReader.readLine();
                    while (line != null && lines.size() < lineCount) {
                        lines.add(line);
                        line = dataReader.readLine();
                    }

                    dataReader.close();
                    in.close();
                }

            }
        }
        return lines;
    }
}
