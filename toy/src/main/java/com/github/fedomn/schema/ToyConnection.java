package com.github.fedomn.schema;

import com.github.fedomn.util.Util;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ToyConnection {
    public static final ToyConnection INSTANCE = new ToyConnection();

    public Connection create(String model) throws SQLException {
        Properties info = new Properties();
        info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        info.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        info.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        info.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL.toString());
        info.put(CalciteConnectionProperty.TOPDOWN_OPT.camelName(), Boolean.TRUE.toString());
        String path = Util.filePath(model);
        info.put("model", path);
        return DriverManager.getConnection("jdbc:calcite:", info);
    }
}
