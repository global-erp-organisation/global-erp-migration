package com.camlait.global.erp.migration.dao;

public class MysqlConnection extends AbstractConnection {
    
    private final static String DRIVER = "com.mysql.jdbc.Driver";
    private final static String URL = "jdbc:mysql://localhost:3306/demo";
    private final static String USER = "root";
    private final static String PASSWORD = "ephesus";
    
    public MysqlConnection() {
        setDbServer(new DatabaseServer(DRIVER, URL, USER, PASSWORD));
    }
    
}
