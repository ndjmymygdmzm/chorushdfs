package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.HdfsVersion;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.*;


public class HdfsCachedPluginBuilderTest {

    private HdfsPluginBuilder builder;

    @Before
    public void setUp() throws Exception {
        builder = spy(new HdfsPluginBuilder());
    }

    @Test
    public void testLoadPluginWhenNewVersion() throws Exception {
        new HdfsCachedPluginBuilder(builder).build(HdfsVersion.V1);
        verify(builder).build(HdfsVersion.V1);
    }

    @Test
    public void testLoadPluginFromCache() {
        HdfsCachedPluginBuilder hdfsCachedPluginLoader = new HdfsCachedPluginBuilder(builder);
        HdfsPluginLoader first = hdfsCachedPluginLoader.build(HdfsVersion.V1);
        verify(builder).build(HdfsVersion.V1);

        reset(builder);
        HdfsPluginLoader second = hdfsCachedPluginLoader.build(HdfsVersion.V0201GP);
        verify(builder).build(HdfsVersion.V0201GP);

        reset(builder);
        HdfsPluginLoader firstAgain = hdfsCachedPluginLoader.build(HdfsVersion.V1);
        verifyZeroInteractions(builder);
        reset(builder);

        assertEquals(first, firstAgain);
        assertNotSame(first, second);
    }
}