package com.github.fedomn.schema;

import org.apache.calcite.util.Util;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ToySchemaTest {

    @Test
    void scalar_test() throws SQLException {
        Statement statement = prepareStatement();
        ResultSet resultSet = statement.executeQuery("select 2*55");
        resultSet.next();

        assertEquals(110, resultSet.getInt(1));
    }

    @Test
    void basic_test() throws SQLException {
        Statement statement = prepareStatement();
        ResultSet resultSet = statement.executeQuery("select * from \"user\"");

        final List<String> lines = new ArrayList<>();
        collect(lines, resultSet);
        assertEquals("[id=1; name=u1, id=2; name=u2, id=3; name=u3]", lines.toString());
    }


    private Statement prepareStatement() throws SQLException {
        Connection connection = ToyConnection.INSTANCE.create("model.json");
        return connection.createStatement();
    }

    private static void collect(List<String> result, ResultSet resultSet)
            throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            buf.setLength(0);
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for (int i = 1; i <= n; i++) {
                buf.append(sep)
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getString(i));
                sep = "; ";
            }
            result.add(Util.toLinux(buf.toString()));
        }
    }
}
