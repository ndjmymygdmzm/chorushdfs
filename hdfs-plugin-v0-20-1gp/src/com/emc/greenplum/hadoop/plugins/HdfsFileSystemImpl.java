package com.emc.greenplum.hadoop.plugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class HdfsFileSystemImpl extends HdfsFileSystemPlugin {
    private FileSystem fileSystem;

    @Override
    public Connection getHiveConnection(String host, String user, String password) throws Exception {
        return null;
    }

    @Override
    public Connection getHiveKerberosConnection(String host, String user, String principal, String keytab) throws Exception {
        return null;
    }

    @Override
    public void loadFileSystem(String host, String port, String username, boolean isHA, boolean isMapR, List<HdfsPair> parameters, String connectionName, String chorusUsername) {
        loadHadoopClassLoader();

        Configuration config = new Configuration();
        config.set("fs.default.name", buildHdfsPath(host, port, isHA));
        config.set("hadoop.job.ugi", username + ", " + username);
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        if (parameters != null && parameters.size() > 0) {
            for (HdfsPair pair : parameters) {
                config.set(pair.getKey(), pair.getValue());
            }
        }

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

        if (files != null) {
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
        }
        return lines;
    }

    @Override
    public HdfsEntityDetails details(String path) throws IOException {
        FileStatus status = fileSystem.getFileStatus(new Path(path));
        return new HdfsEntityDetails(
                status.getOwner(),
                status.getGroup(),
                status.getAccessTime(),
                status.getModificationTime(),
                status.getBlockSize(),
                status.getLen(),
                status.getReplication(),
                status.getPermission().toString()
        );
    }

    @Override
    public boolean importData(String path, InputStream is, boolean overwrite) throws IOException {
        OutputStream os = fileSystem.create(new Path(path), overwrite);
        IOUtils.copyBytes(is, os, 4096, true);
        return true;
    }

    @Override
    public boolean delete(String path) throws IOException {
        return fileSystem.delete(new Path(path), true);
    }
}
