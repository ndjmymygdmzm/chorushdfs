package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;

import java.io.IOException;
import java.util.List;


public interface HdfsFileSystem {
    void loadFileSystem(String host, String port, String username, boolean isHA, List<HdfsPair> parameters);

    void closeFileSystem();

    List<HdfsEntity> list(String path) throws IOException;

    boolean loadedSuccessfully();

    void setClassLoader(JarClassLoader classLoader);

    List<String> getContent(String path, int lineCount) throws IOException;
}
