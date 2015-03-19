package com.emc.greenplum.hadoop.plugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AnnotatedSecurityInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

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

        config.set("fs.defaultFS", buildHdfsPath(host, port, isHA));
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("simple")) {
            config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
        }

        if (parameters != null && parameters.size() > 0) {
            for (HdfsPair pair : parameters) {
                config.set(pair.getKey(), pair.getValue());
            }
        }

        UserGroupInformation.setConfiguration(config);

        try {
            if (isKerberos(config)) {
                SecurityInfo securityInfo = new AnnotatedSecurityInfo();
                SecurityUtil.setSecurityInfoProviders(securityInfo);
                UserGroupInformation ugi = HdfsSecurityUtil.getCachedUserGroupInfo(connectionName, host, config.get(HdfsSecurityUtil.ALPINE_PRINCIPAL));
                if (ugi == null) {
                    ugi = HdfsSecurityUtil.kerberosInitForHDFS(config, host, port, connectionName, isHA, isMapR);
                }
                UserGroupInformation proxyUGI = HdfsSecurityUtil.createProxyUser((chorusUsername == null || chorusUsername.isEmpty() ? username : chorusUsername), ugi);
                fileSystem = HdfsSecurityUtil.getHadoopFileSystem(config, proxyUGI, host, port, connectionName, isHA, isMapR);
                if(!loadedSuccessfully()) {
                    if(checkForExpiredTicket()) {
                        ugi = HdfsSecurityUtil.kerberosInitForHDFS(config, host, port, connectionName, isHA, isMapR);
                        proxyUGI = HdfsSecurityUtil.createProxyUser((chorusUsername == null || chorusUsername.isEmpty() ? username : chorusUsername), ugi);
                        fileSystem = HdfsSecurityUtil.getHadoopFileSystem(config, proxyUGI, host, port, connectionName, isHA, isMapR);
                    }
                }
            } else {
                fileSystem = FileSystem.get(FileSystem.getDefaultUri(config), config, username);
            }

        } catch (Exception e) {
            System.err.println("V3 plugin failed FileSystem.get");
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            restoreOriginalClassLoader();
        }
    }

    private boolean isKerberos(Configuration config) {
        return config.get("hadoop.security.authentication", "").equalsIgnoreCase("kerberos");
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
                System.err.println("V3 plugin cannot load the filesystem root (\"/\")");
                System.err.println(e.getMessage());
                e.printStackTrace(System.err);
                return false;
            }
        } else {
            return false;
        }
    }

    private boolean checkForExpiredTicket() {
        if (fileSystem != null) {
            try {
                fileSystem.exists(new Path("/"));
            }
            catch (IOException e) {
                String message = e.getMessage();
                System.err.println(e.getMessage());
                System.err.println("IOException");
                if(message.contains("InvalidToken") || message.contains("SaslException") || message.contains("GSSException")) {
                    return true;
                }
                return false;
            }
            return false;
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
