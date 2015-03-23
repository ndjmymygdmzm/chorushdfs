package com.emc.greenplum.hadoop.plugins;

import java.lang.Exception;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.logging.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 *
 * Modified from SecurityUtil2.scala in the alpine code.
 */

public class HdfsSecurityUtil {

    public static final String AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    public static Connection getHiveConnection(String host, String user, String password) throws Exception {
        Class.forName(DRIVER_NAME);
        return DriverManager.getConnection(host, user, password);
    }

    public static Connection getHiveKerberosConnection(String host, String user, String principal, String keytab) throws Exception {

        Class.forName(DRIVER_NAME);
        Configuration configuration = new Configuration();
        configuration.set(AUTHENTICATION_KEY, "kerberos");

        UserGroupInformation.setConfiguration(configuration);

        UserGroupInformation ugi = null;
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

        final String finalHost = host;
        final String finalPrincipal = principal;
        final String finalUser = user;
        PrivilegedExceptionAction<Connection> action = new PrivilegedExceptionAction<Connection>(){

        public Connection run() throws Exception {

            Connection conn = DriverManager.getConnection(finalHost + "/;principal=" + finalPrincipal + ";hive.server2.proxy.user=" + finalUser);
            return conn;
            }

        };

        Connection conn = ugi.doAs(action);

        return conn;
    }

}
