package test.com.emc.greenplum.hadoop.plugins;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystemImpl;
import org.junit.Test;
import org.junit.BeforeClass;

import java.io.*;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;


public class HdfsFileSystemImplTest {
    static Properties properties = new Properties();

    @BeforeClass
    public static void onlyOnce() throws Exception {
        InputStream stream = new FileInputStream("src/test/com/emc/greenplum/hadoop/plugin/servers.properties");
        properties.load(stream);
    }

    @Test
    public void testHdfsFileSystemImpl() {
        HdfsFileSystem hdfs = new HdfsFileSystemImpl();
        hdfs.loadFileSystem(properties.getProperty("gphd20.hostname"), properties.getProperty("gphd20.port"), properties.getProperty("gphd20.user"), false, false, null, "", "");
        assertNotNull(hdfs);
    }
}

