package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.Hdfs;
import com.emc.greenplum.hadoop.HdfsVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class IntegrationTest extends AbstractPluginTest {

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
        Hdfs hdfs = hdfsForKey("mapr");
        assertThat(hdfs.getVersion(), is(HdfsVersion.V0202MAPR));
        assertThat(hdfs.list("/").size(), is(not(0)));
    }

    @Test
    public void testApache12() throws Exception {
        Hdfs hdfs = hdfsForKey("apache12");
        assertThat(hdfs.getVersion(), is(HdfsVersion.V1));
        assertThat(hdfs.list("/").size(), is(not(0)));
    }

    @Test
    public void testCDH4() throws Exception {
        Hdfs hdfs = hdfsForKey("cdh4");
        assertThat(hdfs.getVersion(), is(HdfsVersion.V2));
        assertThat(hdfs.list("/").size(), is(not(0)));
    }

    @Test
    public void testGivingUpWhenTheSpecifiedVersionDoesNotConnect() throws Exception {
        Hdfs hdfs = new Hdfs(ps.getProperty("cdh4.hostname"), ps.getProperty("cdh4.port"), ps.getProperty("cdh4.user"), HdfsVersion.V1, false, null);
        assertNull(hdfs.list("/"));
    }

    @Test
    public void testFindingTheCorrectVersionWhenNullIsPassed() throws Exception {
        Hdfs hdfs = new Hdfs(ps.getProperty("cdh4.hostname"), ps.getProperty("cdh4.port"), ps.getProperty("cdh4.user"), (HdfsVersion) null, false, null);
        assertThat(hdfs.getVersion(), is(HdfsVersion.V2));
        assertNotNull(hdfs.list("/"));
    }

    @Test
    public void testFindNonExistentServerVersion() throws Exception {
        Hdfs hdfs = new Hdfs("this.doesnt.exist.com", "1234", "root", false, null);
        assertNull(hdfs.getVersion());
    }

}
