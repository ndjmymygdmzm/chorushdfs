package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugin.HdfsCachedPluginBuilder;
import com.emc.greenplum.hadoop.plugin.HdfsPluginBuilder;
import com.emc.greenplum.hadoop.plugins.HdfsEntity;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Hdfs {
    public static int timeout = 5;

    private static HdfsCachedPluginBuilder pluginLoader;
    private static PrintStream loggerStream = System.out;
    private final HdfsFileSystem fileSystem;
    private HdfsVersion version;

    public static void setLoggerStream(PrintStream stream) {
        loggerStream = stream;
    }

    public Hdfs(String host, String port, String username ) {
        this(host, port, username, detectVersion(host, port, username));
    }

    public Hdfs(String host, String port, String username, HdfsVersion version) {
        if (checkVersion(host, port, username, version)) {
            this.version = version;
        } else {
            this.version = detectVersion(host, port, username);
        }
        fileSystem = loadFileSystem(host, port, username);
    }

    public Hdfs(String host, String port, String username, String versionName) {
        this(host, port, username, HdfsVersion.findVersion(versionName));
    }

    public HdfsVersion getVersion() {
        return version;
    }

    public List<HdfsEntity> list(final String path) {
        return protectTimeout(new Callable<List<HdfsEntity>>() {
            public List<HdfsEntity> call() {
                try {
                    return fileSystem.list(path);
                } catch (IOException e) {
                    return new ArrayList<HdfsEntity>();
                }
            }
        });
    }

    public List<String> content(final String path, final int lineCount) throws IOException {
        return protectTimeout(new Callable<List<String>>() {
            public List<String> call() {
                try {
                    return fileSystem.getContent(path, lineCount);
                } catch (IOException e) {
                    return new ArrayList<String>();
                }
            }
        });
    }

    public void closeFileSystem() {
        protectTimeout(new Callable() {
            @Override
            public Object call() throws Exception {
                if (fileSystem != null) fileSystem.closeFileSystem();
                return null;
            }
        });
    }

    public void finalize() {
        closeFileSystem();
    }

    private static synchronized <T> T protectTimeout(Callable<T> command) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(command);

        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace(loggerStream);
            return null;
        } finally {
            future.cancel(true);
            executor.shutdownNow();
        }
    }


    private static HdfsCachedPluginBuilder getPluginLoader() {
        if (pluginLoader == null) {
            pluginLoader = new HdfsCachedPluginBuilder(new HdfsPluginBuilder());
        }
        return pluginLoader;
    }

    private static HdfsVersion detectVersion(String host, String port, String username) {
        for (HdfsVersion version : HdfsVersion.values()) {
            if (checkVersion(host, port, username, version)) {
                return version;
            }
        }
        return null;
    }

    private static boolean checkVersion(final String host, final String port, final String username, HdfsVersion version) {
        if (version == null) { return false; }
        HdfsFileSystem fileSystem = getPluginLoader().fileSystem(version);

        final HdfsFileSystem fileSystem1 = fileSystem;
        protectTimeout(new Callable() {
            public Object call() {
                fileSystem1.loadFileSystem(host, port, username);
                return  null;
            }
        });

        if (fileSystem.loadedSuccessfully()) {
            fileSystem.closeFileSystem();
            return true;
        } else {
            return false;
        }
    }

    private HdfsFileSystem loadFileSystem(String host, String port, String username) {
        if (version == null) {
            return null;
        }

        HdfsFileSystem fileSystem = getPluginLoader().fileSystem(version);
        fileSystem.loadFileSystem(host, port, username);
        return fileSystem;
    }
}
