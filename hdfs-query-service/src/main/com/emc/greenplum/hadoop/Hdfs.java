package com.emc.greenplum.hadoop;

import com.emc.greenplum.hadoop.plugin.HdfsCachedPluginBuilder;
import com.emc.greenplum.hadoop.plugin.HdfsPluginBuilder;
import com.emc.greenplum.hadoop.plugins.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
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

    public Hdfs(String host, String port, String username, boolean isHA, List<HdfsPair> parameters) {
        this(host, port, username, detectVersion(host, port, username, isHA, parameters), isHA, parameters);
    }

    public Hdfs(String host, String port, String username, HdfsVersion version, boolean isHA, List<HdfsPair> parameters) {
        this(host, port, username, version, isHA, parameters, "");
    }

    public Hdfs(String host, String port, String username, HdfsVersion version, boolean isHA, List<HdfsPair> parameters, String connectionName) {
         if(version == null){
             this.version = detectVersion(host, port, username, isHA, parameters);
         }
         else if (checkVersion(host, port, username, version, isHA, parameters)) {
             this.version = version;
         }

         this.fileSystem = loadFileSystem(host, port, username, isHA, parameters, connectionName);
     }

    public Hdfs(String host, String port, String username, String versionName, boolean isHA, List<HdfsPair> parameters) {
        this(host, port, username, HdfsVersion.findVersion(versionName), isHA, parameters);
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

    public HdfsEntityDetails details(final String path) {
        return protectTimeout(new Callable<HdfsEntityDetails>() {
            public HdfsEntityDetails call() {
                try {
                    return fileSystem.details(path);
                } catch (IOException e) {
                    return new HdfsEntityDetails();
                }
            }
        });
    }

    public HdfsActionResult importData(final String path, final InputStream is, final boolean overwrite) throws IOException {
        return execute(new Callable<HdfsActionResult>() {
            @Override
            public HdfsActionResult call() throws IOException {
                try {
                    boolean success = fileSystem.importData(path, is, overwrite);
                    return new HdfsActionResult(success);
                } catch (UndeclaredThrowableException ute) {
                    Exception e = (Exception) ute.getCause().getCause();
                    return new HdfsActionResult(false, e.getMessage(), e);
                }
            }
        });
    }

    public boolean delete(final String path) {
        return protectTimeout(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return fileSystem.delete(path);
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


    private static HdfsCachedPluginBuilder getPluginLoader() {
        if (pluginLoader == null) {
            pluginLoader = new HdfsCachedPluginBuilder(new HdfsPluginBuilder());
        }
        return pluginLoader;
    }

    private static HdfsVersion detectVersion(String host, String port, String username, boolean isHA, List<HdfsPair> parameters) {
        for (HdfsVersion version : HdfsVersion.values()) {
            if (checkVersion(host, port, username, version, isHA, parameters)) {
                return version;
            }
        }
        return null;
    }

    private static boolean checkVersion(final String host, final String port, final String username, HdfsVersion version, final boolean isHA, final List<HdfsPair> parameters) {
        if (version == null) { return false; }
        HdfsFileSystem fileSystem = getPluginLoader().fileSystem(version);

        final HdfsFileSystem fileSystem1 = fileSystem;
        protectTimeout(new Callable() {
            public Object call() {
                fileSystem1.loadFileSystem(host, port, username, isHA, parameters);
                return null;
            }
        });

        return fileSystem.loadedSuccessfully();
    }

    private HdfsFileSystem loadFileSystem(String host, String port, String username, boolean isHA, List<HdfsPair> parameters) {
        if (version == null) {
            return null;
        }

        HdfsFileSystem fileSystem = getPluginLoader().fileSystem(version);
        fileSystem.loadFileSystem(host, port, username, isHA, parameters);
        return fileSystem;
    }
}
