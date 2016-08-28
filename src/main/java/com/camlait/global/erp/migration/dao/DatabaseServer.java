package com.camlait.global.erp.migration.dao;

import lombok.Data;

@Data
public class DatabaseServer {
    private String driver;
    private String url;
    private String user;
    private String password;

    public DatabaseServer(String driver, String url, String user, String password) {
        super();
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }
}
