package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.plugins.HdfsActionResult;
import com.emc.greenplum.hadoop.plugins.HdfsEntityDetails;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class V4PluginTest extends AbstractPluginTest {

    public static final String TARGET_PATH = "/tmp/servers.properties";

    @Before
    public void setUp() throws Exception {
        hdfs = hdfsForKey("cdh5");
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

    @Test
    public void testContents() throws Exception {
        List<String> content = hdfs.content("/csv/credit.csv", 10);
        assertThat(content.size(), is(10));
    }

    @Test
    public void testImportData() throws Exception {
        it("imports when the user has permission and the file does not exist");

        clean();

        InputStream is = getClass().getResourceAsStream("/servers.properties");
        boolean a = hdfs.importData(TARGET_PATH, is, false).isSuccess();
        assertThat(a, is(true));

        List<String> content = hdfs.content(TARGET_PATH, 10);
        assertThat(content.size(), is(10));

        clean();
    }

    @Test
    public void testImportDataFailsIfFileExistsWithoutOverwrite() throws Exception {
        clean();

        InputStream is = getClass().getResourceAsStream("/servers.properties");
        InputStream is2 = new ByteArrayInputStream("123".getBytes());
        boolean first = hdfs.importData(TARGET_PATH, is, false).isSuccess();
        boolean second = hdfs.importData(TARGET_PATH, is2, false).isSuccess();
        assertThat(first, is(true));
        assertThat(second, is(false));
        assertThat(hdfs.content(TARGET_PATH, 1).get(0), not(containsString("123")));

        clean();
    }


    @Test
    public void testImportDataOverwritesWithOverwrite() throws Exception {
        clean();

        InputStream is = getClass().getResourceAsStream("/servers.properties");
        InputStream is2 = new ByteArrayInputStream("123".getBytes());
        boolean first = hdfs.importData(TARGET_PATH, is, false).isSuccess();
        boolean second = hdfs.importData(TARGET_PATH, is2, true).isSuccess();
        assertThat(first, is(true));
        assertThat(second, is(true));
        assertThat(hdfs.content(TARGET_PATH, 1).get(0), is(containsString("123")));

        clean();
    }

    @Test
    public void testFailureIncludesMessageAndExceptionInWrapper() throws Exception {
        clean();

        InputStream is = getClass().getResourceAsStream("/servers.properties");
        InputStream is2 = new ByteArrayInputStream("123".getBytes());
        hdfs.importData(TARGET_PATH, is, false).isSuccess();
        HdfsActionResult second = hdfs.importData(TARGET_PATH, is2, false);

        assertThat(second.isSuccess(), is(false));
        assertThat(second.getMessage(), is(notNullValue()));
        assertThat(second.getException(), is(notNullValue()));
        assertThat(hdfs.content(TARGET_PATH, 1).get(0), not(containsString("123")));

        clean();

    }

    private void clean() {
        try { hdfs.delete(TARGET_PATH); } catch (Exception e) { /* nom */ }
    }
}
