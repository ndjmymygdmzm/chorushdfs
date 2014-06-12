package com.emc.greenplum.hadoop.plugins;

import org.xeustechnologies.jcl.JarClassLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public abstract class HdfsFileSystemPlugin implements HdfsFileSystem {
    protected JarClassLoader hadoopCl;
    protected ClassLoader originalClassLoader;

    @Override
    public void setClassLoader(JarClassLoader classLoader) {
        hadoopCl = classLoader;
    }

    @Override
    public abstract List<String> getContent(String path, int lineCount) throws IOException;

    @Override
    public abstract HdfsEntityDetails details(String path) throws IOException;

    @Override
    public abstract boolean importData(String path, InputStream is, boolean overwrite) throws IOException;

    @Override
    public abstract boolean delete(String path) throws IOException;

    protected void restoreOriginalClassLoader() {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
    }

    protected void loadHadoopClassLoader() {
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(hadoopCl);
    }

    protected String buildHdfsPath(String host, String port, boolean isHA) {
        if (isHA) {
            return "hdfs://" + host;
        } else {
            return "hdfs://" + host + ":" + port;
        }
    }

}
