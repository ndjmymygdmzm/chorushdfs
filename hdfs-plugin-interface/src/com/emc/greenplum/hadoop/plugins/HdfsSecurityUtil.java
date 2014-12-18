package com.emc.greenplum.hadoop.plugins;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

//import scala.util.{ Failure, Success, Try }

/**
 *
 * Created by chester on 11/18/14.
 */

public class HdfsSecurityUtil {
    private final static Logger logger = Logger.getLogger(HdfsSecurityUtil.class);

    public static final String METHOD_DOAS = "doAs";
    public static final String METHOD_CREATE_REMOTE_USER = "createRemoteUser";
    public static final String HDFS_DELEGATION_TOKEN = "hdfs.delegation.token";
    public static final String ALPINE_KEYTAB    =  "alpine.keytab";
    public static final String ALPINE_PRINCIPAL =  "alpine.principal";
    public static final String AUTHENTICATION_KEY = "hadoop.security.authentication";

    // private final Map<String, String> defaultConf = com.alpine.configuration.DefaultHadoopConfig.defaultKerberosConfig();
    //private final Map<String, String> defaultConf = new HashMap<String, String>();


    // import scala.collection.mutable
    private static Map<String, UserGroupInformation> ugiMap;

    public static UserGroupInformation getCachedUserGroupInfo(String connName, String principal) {
        return ugiMap.get(connName + "_" + principal);
    }

    //why do we use reflection? make sure works with MR1 ?

    public static UserGroupInformation createRemoteUser(String userName) {

        try {
            if (userName != null && userName.trim().length() > 0) {
                Method[] methods = UserGroupInformation.class.getMethods();
                for (Method method : methods) {
                    if (method.getName().equals(METHOD_CREATE_REMOTE_USER) && method.getParameterTypes()[0].equals(String.class)) {
                        return (UserGroupInformation)method.invoke(userName);
                    }
                }
            }
            return null;
        }
        catch(Exception e) {
            return null;
        }

    }

    public static UserGroupInformation createProxyUser(String user, UserGroupInformation loginUser) {
        return UserGroupInformation.createProxyUser(user, loginUser);
    }

    public static UserGroupInformation kerberosInitForHDFS(Configuration configuration, String host, String port, String connectionName, boolean isHA) throws Exception {
        //System.out.println("connection.getHdfsPrincipal = " + connection.getHdfsPrincipal());
        //System.out.println("connection.getHdfsKeyTab = " + connection.getHdfsKeyTab());

        String principal = configuration.get(ALPINE_PRINCIPAL);
        String keyTab = configuration.get(ALPINE_KEYTAB);

        UserGroupInformation ugi = kerberosLogin(principal, keyTab, configuration, host, port, connectionName, isHA);
        addHDFSDelegationToken(configuration, ugi, host, port, isHA);
        return ugi;
    }

    //this may only needed when Web send a HDFS token string, may not needed here.
    public static UserGroupInformation addHDFSDelegationToken(UserGroupInformation ugi, String delegationToken, Configuration configuration) throws Exception {

        if (ugi.getTokens().isEmpty()) {
            Token<DelegationTokenIdentifier> dfsToken = new Token<DelegationTokenIdentifier>();
            dfsToken.decodeFromUrlString(delegationToken);
            //set the token here so it can be used for downstream configuration
            configuration.set(HDFS_DELEGATION_TOKEN, delegationToken);
            UserGroupInformation.setConfiguration(configuration);
            if (ugi.addToken(dfsToken)) {
                logger.info("HDFS Token added to hadoop user group information");
            }
        }

        return ugi;
    }

    public static UserGroupInformation createUserForHDFS(String user, Configuration configuration, String host, String port, String connectionName, boolean isHA) throws Exception {
        if (configuration.get(AUTHENTICATION_KEY).equals("kerberos")) {
            UserGroupInformation loginUser = kerberosInitForHDFS(configuration, host, port, connectionName, isHA);
            return createProxyUser(user, loginUser);
        }
        else {
            return createRemoteUser(user);
        }
    }

    public static void addHDFSDelegationToken(Configuration configuration, UserGroupInformation ugi, String hostname, String port, boolean isHA) throws Exception {
        FileSystem fs = getHadoopFileSystem(configuration, ugi, hostname, port, isHA);
        //currently always fail here.
        logger.info("get Delegation token from FileSystem for user" + ugi.getShortUserName());
        System.out.println("========>ugi.getRealAuthenticationMethod = " + ugi.getAuthenticationMethod());

        Token<DelegationTokenIdentifier> dfsToken = (Token<DelegationTokenIdentifier>)fs.getDelegationToken(ugi.getShortUserName());
        ugi.addToken(new Text(HDFS_DELEGATION_TOKEN), dfsToken);
    }

    /* TODO */
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

    public static UserGroupInformation kerberosLogin(String principal, String keyTabLocation, Configuration configuration, String host, String port, String connectionName, boolean isHA) throws Exception{

        String key = connectionName + "_" + principal;

        if (principal == null || principal.isEmpty()) {
            throw new NullPointerException(" Principal is null");
        }
        if (keyTabLocation == null || keyTabLocation.isEmpty()) {
            throw new NullPointerException("keyTab location is null");
        }

        // note: toHadoopConfiguration also calls
        // for setConfiguration (via reflection) for old APIs.
        UserGroupInformation.setConfiguration(configuration);

        String realHost = (isHA || host.equals("localhost")) ? host : InetAddress.getLocalHost().getHostName();
        String loginPrincipal = getReplacedPrincipal(principal, realHost, Integer.parseInt(port));
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(loginPrincipal, keyTabLocation);

        ugiMap.put(key, ugi);

        return ugi;
    }

    /*
    public String getDelegationTokenReNewer(String principal, String target) throws Exception {
        if (UserGroupInformation.isSecurityEnabled()) {
            return getReplacedPrincipal(principal, target);

            /* TODO: IS THIS RELEVANT??
            String reNewer = realPrincipal.toOption.getOrElse(principal.split("[/@]")(0))

            if (realPrincipal.isFailure) {
                val t = realPrincipal.failed.get
                logger.error(t.getMessage, t)
            }
            else {
                //logger.info(s"Delegation Token ReNewer details: Principal=$principal,Target= $target,ReNewer=$reNewer")
            }

            reNewer

        }
        else {
            return UserGroupInformation.getCurrentUser().getShortUserName();
        }
    }
*/

    public static String getReplacedPrincipal(String principal, String host, int port) throws Exception {
        System.out.println("host = " + host);
        System.out.println("port = " + port);
        String addr = NetUtils.createSocketAddr(host, port).getHostName();
        System.out.println("addr = " + addr);
        return org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principal, addr);

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

    /*
        def performPrivilegedAction[T](ugi: UserGroupInformation)(f: => T): T = {

        def handleException(ex: Throwable): Nothing = {
        ex match {
        case e: InvocationTargetException =>
        e.getTargetException match {
        case t: Exception => t match {
        case pe: PrivilegedActionException => throw pe.getException
        case oe: Exception => throw oe
        }
        }
        case e: Throwable => throw e
        }
        }

        Try {
        val action: PrivilegedExceptionAction[T] = new PrivilegedExceptionAction[T] {
        def run: T = {
        f
        }
        }
        ugi.doAs(action)
        } match {
        case Success(t) => t
        case Failure(ex) => handleException(ex)
        }
        }

        def retry[T](connection: HadoopConnection, maxRetry: Int = 3)(f: => T): T = {

        Try(f) match {
        case Success(t) => t
        case Failure(ex) =>
        if (!ex.isInstanceOf[org.apache.hadoop.security.token.SecretManager.InvalidToken])
        throw ex
        else {
        if (connection.isUseHA && connection.isKerberos) {
        if (maxRetry > 0) {
        retry(connection, maxRetry - 1)(f)
        } else
        throw ex
        } else
        throw ex
        }
        }
        }

        def retryPrivilegedAction[T](connection: HadoopConnection, ugi: UserGroupInformation, maxTry: Int = 3)(f: => T): T = {
        retry(connection, maxTry) {
        SecurityUtil2.performPrivilegedAction(ugi)(f)
        }
        }
    */
}
//  object SecurityUtil2 extends SecurityUtil2
