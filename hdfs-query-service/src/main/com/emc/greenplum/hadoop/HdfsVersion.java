package com.emc.greenplum.hadoop;


public enum HdfsVersion {
    V1("1.0.0", "META-INF/plugins/hdfs-plugin-v1-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-lang-2.4.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/hadoop-core-1.0.3-gphd-1.2.0.0.jar"
    }),
    V0201GP("0.20.1gp", "META-INF/plugins/hdfs-plugin-v0-20-1gp-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/hadoop-0.20.1gp-core.jar"
    }),
    V2("2.0.0", "META-INF/plugins/hdfs-plugin-v2-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-lang-2.5.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/commons-cli-1.2.jar",
            "META-INF/external-deps/slf4j-log4j12-1.6.1.jar",
            "META-INF/external-deps/slf4j-api-1.6.1.jar",
            "META-INF/external-deps/log4j-1.2.16.jar",
            "META-INF/external-deps/protobuf-java-2.4.0a.jar",
            "META-INF/external-deps/guava-11.0.2.jar",
            "META-INF/external-deps/hadoop-auth-2.0.2-alpha-gphd-2.0.1.jar",
            "META-INF/external-deps/hadoop-annotations-2.0.2-alpha-gphd-2.0.1.jar",
            "META-INF/external-deps/hadoop-common-2.0.2-alpha-gphd-2.0.1.jar",
            "META-INF/external-deps/hadoop-hdfs-2.0.2-alpha-gphd-2.0.1.jar"
    }),
    // some jars are provided to external-deps by v2 plugin lib
    V3("3.0.0", "META-INF/plugins/hdfs-plugin-v3-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-lang-2.5.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/commons-cli-1.2.jar",
            "META-INF/external-deps/commons-codec-1.4.jar",
            "META-INF/external-deps/slf4j-log4j12-1.6.1.jar",
            "META-INF/external-deps/slf4j-api-1.6.1.jar",
            "META-INF/external-deps/log4j-1.2.16.jar",
            "META-INF/external-deps/protobuf-java-2.4.0a.jar",
            "META-INF/external-deps/guava-13.0.1.jar",
            "META-INF/external-deps/hadoop-auth-2.0.5-alpha-gphd-2.1.0.0.jar",
            "META-INF/external-deps/hadoop-annotations-2.0.5-alpha-gphd-2.1.0.0.jar",
            "META-INF/external-deps/hadoop-common-2.0.5-alpha-gphd-2.1.0.0.jar",
            "META-INF/external-deps/hadoop-hdfs-2.0.5-alpha-gphd-2.1.0.0.jar"
    }),
    V4("4.0.0", "META-INF/plugins/hdfs-plugin-v4-0.0.1.jar", new String[] {
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/commons-lang-2.5.jar",
            "META-INF/external-deps/commons-configuration-1.6.jar",
            "META-INF/external-deps/commons-cli-1.2.jar",
            "META-INF/external-deps/commons-codec-1.4.jar",
            "META-INF/external-deps/commons-io-2.1.jar",
            "META-INF/external-deps/slf4j-log4j12-1.7.5.jar",
            "META-INF/external-deps/slf4j-api-1.7.5.jar",
            "META-INF/external-deps/log4j-1.2.16.jar",
            "META-INF/external-deps/protobuf-java-2.5.0.jar",
            "META-INF/external-deps/guava-11.0.2.jar",
            "META-INF/external-deps/hadoop-auth-2.2.0-cdh5.0.0-beta-1.jar",
            "META-INF/external-deps/hadoop-annotations-2.2.0-cdh5.0.0-beta-1.jar",
            "META-INF/external-deps/hadoop-common-2.2.0-cdh5.0.0-beta-1.jar",
            "META-INF/external-deps/hadoop-hdfs-2.2.0-cdh5.0.0-beta-1.jar",
            "META-INF/external-deps/hive-exec-0.13.1-cdh5.2.0.jar",
            "META-INF/external-deps/hive-jdbc-0.13.1-cdh5.2.0.jar",
            "META-INF/external-deps/hive-metastore-0.13.1-cdh5.2.0.jar",
            "META-INF/external-deps/hive-service-0.13.1-cdh5.2.0.jar",
            "META-INF/external-deps/hive-shims-0.13.1-cdh5.2.0.jar"
    }),
    V0202MAPR("0.20.2mr", "META-INF/plugins/hdfs-plugin-v0-20-2mr-0.0.1.jar", new String []{
            "META-INF/external-deps/commons-logging-1.1.1.jar",
            "META-INF/external-deps/maprfs-0.1.jar",
            "META-INF/external-deps/zookeeper-3.3.2.jar",
            "META-INF/external-deps/hadoop-0.20.2mr-core.jar",
            "META-INF/external-deps/hive-exec-0.13.0-mapr-1405.jar",
            "META-INF/external-deps/hive-jdbc-0.13.0-mapr-1405.jar",
            "META-INF/external-deps/hive-metastore-0.13.0-mapr-1405.jar",
            "META-INF/external-deps/hive-service-0.13.0-mapr-1405.jar",
            "META-INF/external-deps/hive-shims-0.13.0-mapr-1405.jar"
    });

    private String pluginJar;
    private String name;
    private String [] dependencies;


    HdfsVersion(String name, String pluginJar, String[] dependencies) {
        this.pluginJar = pluginJar;
        this.name = name;
        this.dependencies = dependencies;
    }

    public static HdfsVersion findVersion(String versionName) {
        for(HdfsVersion hdfsVersion: HdfsVersion.values()) {
            if(hdfsVersion.getName().equals(versionName)) {
                return hdfsVersion;
            }
        }

        return null;
    }

    public String getPluginJar() {
        return pluginJar;
    }

    public String getName() {
        return name;
    }

    public String[] getDependencies() {
        return dependencies;
    }
}
