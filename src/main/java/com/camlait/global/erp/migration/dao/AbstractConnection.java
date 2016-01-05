package com.camlait.global.erp.migration.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractConnection {
    
    private Connection conn;
    private DatabaseServer dbServer;
    
    public ResultSet execute(String sql) {
        if (conn == null)
            connect();
        ResultSet rs = null;
        try {
            Statement stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
        }
        return rs;
    }
    
    private void connect() {
        try {
            Class.forName(dbServer.getDriver());
            conn = DriverManager.getConnection(dbServer.getUrl(), dbServer.getUser(), dbServer.getPassword());
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
    
    public Connection getConn() {
        return conn;
    }
        
    public DatabaseServer getDbServer() {
        return dbServer;
    }
    
    public void setDbServer(DatabaseServer dbServer) {
        this.dbServer = dbServer;
    }
}
