package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public interface HdfsFileSystem {
    void loadFileSystem(String host, String port, String username, boolean isHA, List<HdfsPair> parameters);

    void closeFileSystem();

    boolean loadedSuccessfully();

    void setClassLoader(JarClassLoader classLoader);

    List<HdfsEntity> list(String path) throws IOException;

    List<String> getContent(String path, int lineCount) throws IOException;

    HdfsEntityDetails details(String path) throws IOException;

    boolean importData(String path, InputStream is, boolean overwrite) throws IOException;

    boolean delete(String path) throws IOException;
}
