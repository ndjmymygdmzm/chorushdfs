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

    public static UserGroupInformation createUserForHDFS(String user, Configuration configuration, String host, String port, String connectionName, boolean isHA, String chorusUsername) throws Exception {

        UserGroupInformation loginUser = kerberosInitForHDFS(configuration, host, port, connectionName, isHA);
        return createProxyUser((chorusUsername == null || chorusUsername.isEmpty() ? user : chorusUsername), loginUser);
    }


    // Log in using Kerberos
    public static UserGroupInformation kerberosInitForHDFS(Configuration configuration, String host, String port, String connectionName, boolean isHA) throws Exception {
        String principal = configuration.get(ALPINE_PRINCIPAL);
        String keyTab = configuration.get(ALPINE_KEYTAB);

        UserGroupInformation ugi = kerberosLogin(principal, keyTab, configuration, host, port, connectionName, isHA);
        addHDFSDelegationToken(configuration, ugi, host, port, isHA);
        return ugi;
    }


    // Log in as proxy user based on Chorus username
    public static UserGroupInformation createProxyUser(String user, UserGroupInformation loginUser) throws Exception {
        return UserGroupInformation.createProxyUser(user, loginUser);
    }

    // Perform actual get from HDFS

    public static FileSystem getHadoopFileSystem(Configuration configuration, UserGroupInformation ugi, String hostname, String port, boolean isHA) throws Exception {

        final Configuration finalConfiguration = configuration;
        final String hdfsURL = "hdfs://" + hostname + (isHA ? ":" + Integer.parseInt(port) : "");

        PrivilegedExceptionAction<FileSystem> action = new PrivilegedExceptionAction<FileSystem>(){

            @Override
            public FileSystem run() throws Exception {
                return FileSystem.get(URI.create(hdfsURL), finalConfiguration);
            }
        };
        return (FileSystem)performPrivilegedExceptionAction(ugi, action);

    }

    public static void addHDFSDelegationToken(Configuration configuration, UserGroupInformation ugi, String hostname, String port, boolean isHA) throws Exception{

        if (!hdfsTokenUGIs.contains(ugi)) {
            FileSystem fs = getHadoopFileSystem(configuration, ugi, hostname, port, isHA);

            Token<DelegationTokenIdentifier> dfsToken = (Token<DelegationTokenIdentifier>)fs.getDelegationToken(ugi.getShortUserName());
            ugi.addToken(new Text(HDFS_DELEGATION_TOKEN), dfsToken);
            hdfsTokenUGIs.add(ugi);
        }

    }

    public static String getHDFSPrefixFromUrl(String url) {
        if (url == null || url.isEmpty()) {
            return HDFS_PREFIX;
        }
        else {
            int index = url.indexOf("//");
            if (index < 0 || index - 2 <= 0) {
                return url;
            }
            else {
                return url.substring(0, index - 2);
            }
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
        if (port.isEmpty() || port == "-1") {
            target = getHDFSPrefixFromUrl(hdfsUrl) + InetAddress.getLocalHost().getHostName() + ":50070";
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

    public static Object performPrivilegedExceptionAction(UserGroupInformation ugi,
                                                          PrivilegedExceptionAction  action) throws  Exception {
        Method[] methods = UserGroupInformation.class.getMethods();

        for (Method method : methods) {
            if (method.getName().equals(METHOD_DOAS)
                    && method.getParameterTypes()[0]
                    .equals(PrivilegedExceptionAction.class)) {
                try {
                    return method.invoke(ugi, action);
                } catch (Exception e) {//nullpoint, empty...
                    //itsLogger.error(StackTraceLogger.getErrString(e),e); 80855410
                    if (e instanceof InvocationTargetException) {
                        Throwable targetException = ((InvocationTargetException) e).getTargetException();
                        if (targetException instanceof UndeclaredThrowableException) {
                            Throwable ue = ((UndeclaredThrowableException) targetException).getUndeclaredThrowable();
                            /*if(ue instanceof EmptyFileException){//this is for pivotalhd 11
                                throw (EmptyFileException) ue;
                            }
                            else*/
                            if (ue instanceof PrivilegedActionException) {
                                throw ((PrivilegedActionException) ue).getException();
                            }  else if (ue instanceof Exception) {
                                throw ((Exception) ue) ;//this for the change in CD5, see 69201736
                            }
                            else {
                                throw e;
                            }
                        } else {
                            if (((InvocationTargetException) e).getTargetException() != null
                                    && ((InvocationTargetException) e).getTargetException() instanceof Exception) {
                                throw (Exception) ((InvocationTargetException) e).getTargetException();
                            } else {
                                throw e;
                            }

                        }
                    } else {
                        throw e;
                    }
                }

            }
        }
        return null;
    }

}
