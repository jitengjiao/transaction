package com.j2ee.transaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.UserTransaction;

/**
 * @author jmesnil
 * Updated tortiz
 *
 * This class is an example of use of JOTM within a Distributed DataBase
 */
public class JdbcDistExample {

    private static final String USAGE = "usage: java JdbcDistExample [database1 database2] [commit|rollback] [number]";
    private static final String SQL_REQUEST1 = "select id, amount from t_account";
    private static final String SQL_QUERY1 = "update t_account set amount = amount - 500 where id=1";
    private static final String SQL_REQUEST2 = "select id, amount from t_account";
    private static final String SQL_QUERY2 = "update t_account set amount = amount + 500 where id=1";
    private static final String USER_TRANSACTION_JNDI_NAME = "UserTransaction";
    private static Connection conn1 = null;
    private static Connection conn2 = null;

    private static void printTable(Connection conn, String sqlRequest) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery(sqlRequest);
            int numcols = rset.getMetaData().getColumnCount();
            for (int i = 1; i <= numcols; i++) {
                System.out.print("\t" + rset.getMetaData().getColumnName(i));
            }
            System.out.println();
            while (rset.next()) {
                for (int i = 1; i <= numcols; i++) {
                    System.out.print("\t" + rset.getString(i));
                }
                System.out.println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void updateTable(Connection conn, String sqlQuery, int newValue) {
        try {           
            PreparedStatement pstmt = conn.prepareStatement(sqlQuery);
//            pstmt.setInt(1, newValue);
            pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    	args = new String[]{"mysql1", "mysql2", "commit", "2"};
    	
        if (args.length != 4 || (!args[2].equals("commit") && !args[2].equals("rollback"))) {
            System.out.println(USAGE + "\n");
            System.exit(1);
        }

        String completion = args[2];

        int newValue = 0;
        try {
            newValue = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println(USAGE);
            System.out.println("[number] has to be an integer\n");
            System.exit(1);
        }

        System.out.println("start server");
        DistDatabaseHelper ddbHelper = new DistDatabaseHelper(args[0], args[1]);

        UserTransaction utx = null;
        try {
            System.out.println("create initial context");
            Context ictx = new InitialContext();
            System.out.println("lookup UserTransaction at : " + USER_TRANSACTION_JNDI_NAME);
            utx = (UserTransaction) ictx.lookup(USER_TRANSACTION_JNDI_NAME);
        } catch (Exception e) {
            System.out.println("Exception of type :" + e.getClass().getName() + " has been thrown");
            System.out.println("Exception message :" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        try {
            System.out.println("get a connection");
            conn1 = ddbHelper.getConnection1();
            conn2 = ddbHelper.getConnection2();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("before transaction, table is:");
        printTable(conn1, SQL_REQUEST1);
        printTable(conn2, SQL_REQUEST2);

        try {
            System.out.println("begin a transaction");
            utx.begin();

            System.out.println("update the table");
            updateTable(conn1, SQL_QUERY1, newValue);
            updateTable(conn2, SQL_QUERY2, newValue);

            if (completion.equals("commit")) {
                System.out.println("*commit* the transaction");
                utx.commit();
            } else {
                System.out.println("*rollback* the transaction");
                utx.rollback();
            }
        } catch (Exception e) {
            System.out.println("Exception of type :" + e.getClass().getName() + " has been thrown");
            System.out.println("Exception message :" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        utx = null;

        System.out.println("after transaction, table is:");
        printTable(conn1, SQL_REQUEST1);
        printTable(conn2, SQL_REQUEST2);

        try {
            System.out.println("close connection");
            conn1.close();
            conn2.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn1 = null;
            conn2 = null;
        }

        System.out.println("stop server");
        ddbHelper.stop();

        System.out.println("JDBC example is ok.\n");
        System.exit(0);
    }
}
