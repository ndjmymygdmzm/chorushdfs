package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.plugins.HdfsEntityDetails;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class V1PluginTest extends AbstractPluginTest {

    @Before
    public void setUp() throws Exception {
        hdfs = hdfsForKey("apache12");
    }

    @Test
    public void testHdfsFileSystemImpl() {
        assertThat(hdfs, is(notNullValue()));
    }

    @Test
    public void testDetails() throws Exception {
        HdfsEntityDetails details = hdfs.details("/");
        assertThat("it has the owner", details.getOwner(), is(notNullValue()));
        assertThat("it has the block size", details.getBlockSize(), is(greaterThanOrEqualTo(0l)));
    }

}