package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import org.junit.Before;

import java.io.InputStream;
import java.util.Properties;

public class AbstractPluginTest {

    protected static Properties ps = new Properties();
    protected Hdfs hdfs;

    @Before
    public void onlyOnce() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/servers.properties");
        ps.load(stream);
        stream.close();
    }

    protected Hdfs hdfsForKey(String key) {
        return new Hdfs(
            ps.getProperty(key + ".hostname"),
            ps.getProperty(key + ".port"),
            ps.getProperty(key + ".user"),
            false,
            null
        );
    }

    protected static void it(String message) {
        System.out.println("[it] " + message);
    }
}
