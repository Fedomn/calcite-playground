package com.github.fedomn.schema;

import com.github.fedomn.util.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ToyConnection {
    public static final ToyConnection INSTANCE = new ToyConnection();

    public Connection create(String model) throws SQLException {
        Properties info = new Properties();
        String path = Util.filePath(model);
        info.put("model", path);
        return DriverManager.getConnection("jdbc:calcite:", info);
    }
}
