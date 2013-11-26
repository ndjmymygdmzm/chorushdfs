#!/bin/bash

mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-annotations -Dpackaging=jar -Dversion=2.0.2-alpha-gphd-2.0.1 -Dfile=./hdfs-plugin-v2/lib/hadoop-annotations-2.0.2-alpha-gphd-2.0.1.jar
mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-common -Dpackaging=jar -Dversion=2.0.2-alpha-gphd-2.0.1 -Dfile=./hdfs-plugin-v2/lib/hadoop-common-2.0.2-alpha-gphd-2.0.1.jar
mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-hdfs -Dpackaging=jar -Dversion=2.0.2-alpha-gphd-2.0.1 -Dfile=./hdfs-plugin-v2/lib/hadoop-hdfs-2.0.2-alpha-gphd-2.0.1.jar
mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-auth -Dpackaging=jar -Dversion=2.0.2-alpha-gphd-2.0.1 -Dfile=./hdfs-plugin-v2/lib/hadoop-auth-2.0.2-alpha-gphd-2.0.1.jar

mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-annotations -Dpackaging=jar -Dversion=2.0.5-alpha-gphd-2.1.0.0 -Dfile=./hdfs-plugin-v3/lib/hadoop-annotations-2.0.5-alpha-gphd-2.1.0.0.jar
mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-common -Dpackaging=jar -Dversion=2.0.5-alpha-gphd-2.1.0.0 -Dfile=./hdfs-plugin-v3/lib/hadoop-common-2.0.5-alpha-gphd-2.1.0.0.jar
mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-hdfs -Dpackaging=jar -Dversion=2.0.5-alpha-gphd-2.1.0.0 -Dfile=./hdfs-plugin-v3/lib/hadoop-hdfs-2.0.5-alpha-gphd-2.1.0.0.jar
mvn install:install-file -DgroupId=com.emc.greenplum -DartifactId=hadoop-auth -Dpackaging=jar -Dversion=2.0.5-alpha-gphd-2.1.0.0 -Dfile=./hdfs-plugin-v3/lib/hadoop-auth-2.0.5-alpha-gphd-2.1.0.0.jar

