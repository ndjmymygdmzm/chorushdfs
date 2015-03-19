package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugin.HdfsCachedPluginBuilder;
import com.emc.greenplum.hadoop.plugin.HdfsPluginBuilder;
import com.emc.greenplum.hadoop.plugins.*;

import java.io.IOException;

import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;

import java.lang.SecurityException;
import java.util.logging.*;

public class HiveConnection {

    private static HdfsCachedPluginBuilder pluginLoader;
    private HdfsFileSystem fileSystem;

    public static int timeout = 5;
    private static PrintStream loggerStream = System.out;

    public static void setLoggerStream(PrintStream stream) {
        loggerStream = stream;
    }

    public HiveConnection() {

    }

    public Connection getHiveConnection(final String host, final String user, final String password, final String version) throws Exception {

        HdfsVersion hdfsVersion = HdfsVersion.findVersion(version);

        if(fileSystem == null) {
            fileSystem = getPluginLoader().fileSystem(hdfsVersion);
        }

        return fileSystem.getHiveConnection(host, user, password);
    }

    public Connection getHiveKerberosConnection(final String host, final String user, final String principal, final String keytab, final String version) throws Exception {

        HdfsVersion hdfsVersion = HdfsVersion.findVersion(version);

        if(fileSystem == null) {
            fileSystem = getPluginLoader().fileSystem(hdfsVersion);
        }

        return fileSystem.getHiveKerberosConnection(host, user, principal, keytab);
    }

    private static HdfsCachedPluginBuilder getPluginLoader() {
        if (pluginLoader == null) {
            pluginLoader = new HdfsCachedPluginBuilder(new HdfsPluginBuilder());
        }
        return pluginLoader;
    }

    private static synchronized <T> T protectTimeout(Callable<T> command) {
        return execute(command, true);
    }

    private static synchronized <T> T execute(Callable<T> command) {
        return execute(command, false);
    }

    private static synchronized <T> T execute(Callable<T> command, boolean protectTimeout) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(command);

        try {
            if (protectTimeout) {
                return future.get(timeout, TimeUnit.SECONDS);
            } else {
                return future.get();
            }
        } catch (Exception e) {
            e.printStackTrace(loggerStream);
            return null;
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

}
