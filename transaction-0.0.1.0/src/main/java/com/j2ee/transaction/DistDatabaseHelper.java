package com.j2ee.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.objectweb.jotm.Jotm;

import org.enhydra.jdbc.standard.StandardXADataSource;


/**
 * @author jmesnil
 * updated tortiz
 *
 */
public class DistDatabaseHelper {

    private String password1;
    private String password2;
    private Jotm jotm;
    private XADataSource xads1;
    private XADataSource xads2;
    private String login1;
    private String login2;
    private static final String USER_TRANSACTION_JNDI_NAME = "UserTransaction";

    /**
     * Constructor for DistDatabaseHelper.
     */
    public DistDatabaseHelper(String database1, String database2) {
        Properties props1 = new Properties();
        Properties props2 = new Properties();

        try {
            props1.load(ClassLoader.getSystemResourceAsStream(database1 + ".properties"));
        } catch (Exception e) {
            System.out.println("no properties file found found for " + database1);
            System.exit(1);
        }

        System.out.println("\n" + database1 + " configuration:");
        props1.list(System.out);
        System.out.println("------------------------\n");

        login1 = props1.getProperty("login");
        password1 = props1.getProperty("password");

        try {
            props2.load(ClassLoader.getSystemResourceAsStream(database2 + ".properties"));
        } catch (Exception e) {
            System.out.println("no properties file found found for " + database2);
            System.exit(1);
        }

        System.out.println("\n" + database2 + " configuration:");
        props2.list(System.out);
        System.out.println("------------------------\n");

        login2 = props2.getProperty("login");
        password2 = props2.getProperty("password");

        // Get a transction manager       
        try {
        	// creates an instance of JOTM with a local transaction factory which is not bound to a registry
            jotm = new Jotm(true, false);
            InitialContext ictx = new InitialContext();
            ictx.rebind(USER_TRANSACTION_JNDI_NAME, jotm.getUserTransaction());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        xads1 = new StandardXADataSource();
        try {
            ((StandardXADataSource) xads1).setDriverName(props1.getProperty("driver"));
            ((StandardXADataSource) xads1).setUrl(props1.getProperty("url"));
            ((StandardXADataSource) xads1).setTransactionManager(jotm.getTransactionManager());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        xads2 = new StandardXADataSource();
        try {
            ((StandardXADataSource) xads2).setDriverName(props2.getProperty("driver"));
            ((StandardXADataSource) xads2).setUrl(props2.getProperty("url"));
            ((StandardXADataSource) xads2).setTransactionManager(jotm.getTransactionManager());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Connection getConnection1() throws SQLException {
        XAConnection xaconn = xads1.getXAConnection(login1, password1);
        return xaconn.getConnection();
    }

    public Connection getConnection2() throws SQLException {
        XAConnection xaconn = xads2.getXAConnection(login2, password2);
        return xaconn.getConnection();
    }

    /**
     * Method stop.
     */
    public void stop() {
    	xads1 = null;
        xads2 = null;

        try {
           InitialContext ictx = new InitialContext();
           ictx.unbind(USER_TRANSACTION_JNDI_NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }

        jotm.stop();
        jotm = null;
    }

}

