package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.plugins.HdfsEntityDetails;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class V2PluginTest extends AbstractPluginTest {

    @Before
    public void setUp() throws Exception {
        hdfs = hdfsForKey("gphd02");
    }

    @Test
    public void testHdfsFileSystemImpl() {
        assertThat(hdfs, is(notNullValue()));
    }

    @Test
    public void testDetails() throws Exception {
        HdfsEntityDetails details = hdfs.details("/election92.csv");
        assertThat("it has the owner", details.getOwner(), is(ps.getProperty("gphd02.user")));
    }

}
