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
    private static Map<String, UserGroupInformation> ugiMap = new HashMap<String, UserGroupInformation>();
    private static Set<UserGroupInformation> hdfsTokenUGIs = new HashSet<UserGroupInformation>();

    // Retrieve cached UGI object based on Hadoop connection name and principal
    public static UserGroupInformation getCachedUserGroupInfo(String connName, String principal) {
        return ugiMap.get(connName + "_" + principal);
    }

    // Log in using Kerberos
    public static UserGroupInformation kerberosInitForHDFS(Configuration configuration, String host, String port, String connectionName, boolean isHA, boolean isMapR) throws Exception {
        String principal = configuration.get(ALPINE_PRINCIPAL);
        String keyTab = configuration.get(ALPINE_KEYTAB);

        UserGroupInformation ugi = kerberosLogin(principal, keyTab, configuration, host, port, connectionName, isHA);
        addHDFSDelegationToken(configuration, ugi, host, port, isHA, isMapR);
        return ugi;
    }


    // Log in as proxy user based on Chorus username
    public static UserGroupInformation createProxyUser(String user, UserGroupInformation loginUser) throws Exception {
        return UserGroupInformation.createProxyUser(user, loginUser);
    }

    // Perform actual get from HDFS

    public static FileSystem getHadoopFileSystem(Configuration configuration, UserGroupInformation ugi, String hostname, String port, boolean isHA, boolean isMapR) throws Exception {

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

    public static void addHDFSDelegationToken(Configuration configuration, UserGroupInformation ugi, String hostname, String port, boolean isHA, boolean isMapR) throws Exception{

        if (!hdfsTokenUGIs.contains(ugi)) {
            FileSystem fs = getHadoopFileSystem(configuration, ugi, hostname, port, isHA, isMapR);

            Token<DelegationTokenIdentifier> dfsToken = (Token<DelegationTokenIdentifier>)fs.getDelegationToken(ugi.getShortUserName());
            ugi.addToken(new Text(HDFS_DELEGATION_TOKEN), dfsToken);
            hdfsTokenUGIs.add(ugi);
        }

    }

    // Use Kerberos login and cache resulting UGI
    public static UserGroupInformation kerberosLogin(String principal, String keyTabLocation, Configuration configuration, String host, String port, String connectionName, boolean isHA) throws Exception{

        String key = connectionName + "_" + principal;

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

        ugiMap.put(key, ugi);

        return ugi;
    }


    public static String getReplacedPrincipal(String principal, String host, String port) throws Exception {

        int portNum = (port.isEmpty() || port == "-1") ? 50070 : Integer.parseInt(port);

        String addr = NetUtils.createSocketAddr(host, portNum).getHostName();
        String localprincipal = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principal, addr);
        return localprincipal;

    }

}
