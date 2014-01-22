package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import org.junit.BeforeClass;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class AbstractPluginTest {

    protected static Properties ps = new Properties();
    protected Hdfs hdfs;

    @BeforeClass
    public static void onlyOnce() throws Exception {
        InputStream stream = new FileInputStream("src/test/com/emc/greenplum/hadoop/plugin/servers.properties");
        ps.load(stream);
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
}
