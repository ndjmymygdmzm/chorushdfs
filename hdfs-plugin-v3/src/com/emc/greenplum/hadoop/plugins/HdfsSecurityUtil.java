package com.emc.greenplum.hadoop.plugins;

import java.lang.Exception;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
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

    public static final String METHOD_DOAS = "doAs";
    public static final String METHOD_CREATE_REMOTE_USER = "createRemoteUser";
    public static final String HDFS_DELEGATION_TOKEN = "hdfs.delegation.token";
    public static final String ALPINE_KEYTAB    =  "alpine.keytab";
    public static final String ALPINE_PRINCIPAL =  "alpine.principal";
    public static final String AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String PROPERTY_FS_NAME = "fs.default.name";
    public static final String HDFS_PREFIX = "hdfs://";

    // import scala.collection.mutable
    private static Map<String, HdfsExpirableUGI> ugiMap = new HashMap<String, HdfsExpirableUGI>();

    private static final long EXPIRATION_TIME = 10*60*60*1000;

    // Retrieve cached UGI object based on Hadoop connection name and principal
    public static UserGroupInformation getCachedUserGroupInfo(String connName, String host, String principal) {
        String key = connName + "_" + host + "_" + principal;
        Date now = new Date(System.currentTimeMillis());

        if(ugiMap.containsKey(key) && (now.getTime() - ugiMap.get(key).getTimeCreated().getTime() < EXPIRATION_TIME)) {
            return ugiMap.get(key).getUserGroupInformation();
        }

        return null;
    }

    // Log in using Kerberos
    public static UserGroupInformation kerberosInitForHDFS(Configuration configuration, String host, String port, String connectionName, boolean isHA, boolean isMapR) throws Exception {
        String principal = configuration.get(ALPINE_PRINCIPAL);
        String keyTab = configuration.get(ALPINE_KEYTAB);

        UserGroupInformation ugi = kerberosLogin(principal, keyTab, configuration, host, port, connectionName, isHA, false);
        return ugi;
    }


    // Log in as proxy user based on Chorus username
    public static UserGroupInformation createProxyUser(String user, UserGroupInformation loginUser) throws Exception {
        return UserGroupInformation.createProxyUser(user, loginUser);
    }

    // Perform actual get from HDFS

    public static FileSystem getHadoopFileSystem(Configuration configuration, UserGroupInformation ugi, String hostname, String port, String connectionName, boolean isHA, boolean isMapR) throws Exception {

        final Configuration finalConfiguration = configuration;
        String scheme = isMapR ? "maprfs://" : "hdfs://";
        final String hdfsURL = scheme + hostname + (isHA ? "" : ":" + Integer.parseInt(port));

        PrivilegedExceptionAction<FileSystem> action = new PrivilegedExceptionAction<FileSystem>(){

            @Override
            public FileSystem run() throws Exception {
                return FileSystem.get(URI.create(hdfsURL), finalConfiguration);
            }
        };

        return ugi.doAs(action);
    }

    // Use Kerberos login and cache resulting UGI
    public static UserGroupInformation kerberosLogin(String principal, String keyTabLocation, Configuration configuration, String host, String port, String connectionName, boolean isHA, boolean isReLogin) throws Exception{

        String key = connectionName + "_" + host + "_" + principal;

        if(isReLogin && ugiMap.containsKey(key)) {
            UserGroupInformation oldUgi = ugiMap.get(key).getUserGroupInformation();
            try {
                oldUgi.checkTGTAndReloginFromKeytab();
                return oldUgi;
            }
            catch (Exception e) {
                // otherwise, something went wrong, let's just log them back in
            }
        }

        if (principal == null || principal.isEmpty()) {
            throw new NullPointerException(" Principal is null");
        }
        if (keyTabLocation == null || keyTabLocation.isEmpty()) {
            throw new NullPointerException("keyTab location is null");
        }

        UserGroupInformation.setConfiguration(configuration);
        String hdfsUrl = configuration.get(PROPERTY_FS_NAME);
        String hostName = InetAddress.getLocalHost().getHostName().toLowerCase();
        URI url = URI.create(hdfsUrl);
        String target = url.getScheme() + "://" + hostName + ":";
        if(isHA) {
            target += "50070";
        }
        else {
            target += url.getPort();
        }

        String loginPrincipal = getReplacedPrincipal(principal, InetAddress.getLocalHost().getHostName(), port);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(loginPrincipal, keyTabLocation);

        HdfsExpirableUGI expirableUGI = new HdfsExpirableUGI(ugi);

        ugiMap.put(key, expirableUGI);

        return ugi;
    }


    public static String getReplacedPrincipal(String principal, String host, String port) throws Exception {

        int portNum = (port.isEmpty() || port == "-1") ? 50070 : Integer.parseInt(port);

        String addr = NetUtils.createSocketAddr(host, portNum).getHostName();
        String localprincipal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principal, addr);
        return localprincipal;

    }

}
