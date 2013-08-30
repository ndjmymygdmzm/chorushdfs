package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.HdfsPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;


public class IntegrationTest {

    static Properties properties = new Properties();

    @BeforeClass
    public static void onlyOnce() throws Exception {
        InputStream stream = new FileInputStream("src/test/com/emc/greenplum/hadoop/plugin/servers.properties");
        properties.load(stream);
    }

    @Before
    public void setUp() throws Exception {
        Hdfs.setLoggerStream(new PrintStream(new File("/dev/null")));
        Hdfs.timeout = 1;
    }

    @After
    public void tearDown() throws Exception {
        Hdfs.timeout = 5;
    }

    @Test
    public void testMapRPlugin() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gpmr12.hostname"), properties.getProperty("gpmr12.port"), properties.getProperty("gpmr12.user"), false, null);
        assertEquals(HdfsVersion.V0202MAPR, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd11Plugin() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gphd11.hostname"), properties.getProperty("gphd11.port"), properties.getProperty("gphd11.user"), false, null);
        assertEquals(HdfsVersion.V1, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

//    @Test
//    public void testClouderaWithHA() throws Exception {
//
//        List<HdfsPair> params = new ArrayList<HdfsPair>();
//
//        for (Object o: properties.keySet()) {
//            String key = (String) o;
//            if ( key.startsWith("dfs.") ) {
//                params.add(new HdfsPair(key, properties.getProperty(key)));
//            }
//        }
//
//        Hdfs hdfs = new Hdfs(properties.getProperty("cdhha.hostname"), properties.getProperty("cdhha.port"), properties.getProperty("cdhha.user"), Boolean.valueOf(properties.getProperty("cdhha.ha")), params);
//        assertEquals(HdfsVersion.V2, hdfs.getVersion());
//        assertNotSame(0, hdfs.list("/").size());
//    }

//    @Test
//    public void testGphd12Plugin() throws Exception {
//        Hdfs hdfs = new Hdfs(properties.getProperty("gphd12.hostname"), properties.getProperty("gphd12.port"), properties.getProperty("gphd12.user"), false, null);
//        assertEquals(HdfsVersion.V1, hdfs.getVersion());
//        assertNotSame(0, hdfs.list("/").size());
//    }

    @Test
    public void testGphd20Plugin() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gphd20.hostname"), properties.getProperty("gphd20.port"), properties.getProperty("gphd20.user"), false, null);
        assertEquals(HdfsVersion.V2, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGphd02Plugin() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gphd02.hostname"), properties.getProperty("gphd02.port"), properties.getProperty("gphd02.user"), false, null);
        assertEquals(HdfsVersion.V0201GP, hdfs.getVersion());
        assertNotSame(0, hdfs.list("/").size());
    }

    @Test
    public void testGivingUpWhenTheSpecifiedVersionDoesNotConnect() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gphd02.hostname"), properties.getProperty("gphd02.port"), properties.getProperty("gphd02.user"), HdfsVersion.V1, false, null);
        assertNull(hdfs.list("/"));
    }

    @Test
    public void testFindingTheCorrectVersionWhenNullIsPassed() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gphd02.hostname"), properties.getProperty("gphd02.port"), properties.getProperty("gphd02.user"), (HdfsVersion) null, false, null);
        assertEquals(HdfsVersion.V0201GP, hdfs.getVersion());
        assertNotNull(hdfs.list("/"));
    }

    @Test
    public void testFindNonExistentServerVersion() throws Exception {
        Hdfs hdfs = new Hdfs("this.doesnt.exist.com", "1234", "root", false, null);
        assertNull(hdfs.getVersion());
    }

    @Test
    public void testValidHostnameInvalidCredentials() throws Exception {
        Hdfs hdfs = new Hdfs(properties.getProperty("gphd20.hostname"), "1234", "root", false, null);
        assertNull(hdfs.getVersion());
    }
}
