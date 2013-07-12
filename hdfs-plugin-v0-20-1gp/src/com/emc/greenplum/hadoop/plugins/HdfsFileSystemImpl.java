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
    public void loadFileSystem(String host, String port, String username) {
        loadHadoopClassLoader();

        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfs://" + host + ":" + port);
        config.set("hadoop.job.ugi", username + ", " + username);
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            fileSystem = FileSystem.get(config);
        } catch (IOException e) {
        } finally {
            restoreOriginalClassLoader();
        }
    }

    @Override
    public void closeFileSystem() {
        try {
            fileSystem.closeAll();
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

            entity.setDirectory(fileStatus.isDir());
            entity.setPath(fileStatus.getPath().toUri().getPath());
            entity.setModifiedAt(new Date(fileStatus.getModificationTime()));
            entity.setSize(fileStatus.getLen());

            if (fileStatus.isDir()) {
                try {
                    FileStatus[] contents = fileSystem.listStatus(fileStatus.getPath());
                    entity.setContentCount(contents.length);
                } catch (Exception exception) {
                    entity.setContentCount(-1);
                }
            }

            entities.add(entity);
        }

        return entities;
    }

    @Override
    public boolean loadedSuccessfully() {
        return fileSystem != null;
    }

    @Override
    public List<String> getContent(String path, int lineCount) throws IOException {
        FileStatus[] files = fileSystem.globStatus(new Path(path));
        ArrayList<String> lines = new ArrayList<String>();

        for (FileStatus file : files) {

            if (lines.size() >= lineCount) { break; }

            if (!file.isDir()) {

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
        return lines;
    }
}
