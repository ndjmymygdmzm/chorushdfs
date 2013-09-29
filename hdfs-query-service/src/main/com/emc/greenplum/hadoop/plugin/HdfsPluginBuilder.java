package com.emc.greenplum.hadoop.plugin;

import com.emc.greenplum.hadoop.HdfsVersion;
import com.emc.greenplum.hadoop.plugins.HdfsFileSystem;


public class HdfsPluginBuilder {
    public HdfsPluginLoader build(HdfsVersion version) {
        return new HdfsPluginLoader(version);
    }

    public HdfsFileSystem fileSystem(HdfsVersion version) {
        return build(version).loadObjectFromPlugin();
    }
}
