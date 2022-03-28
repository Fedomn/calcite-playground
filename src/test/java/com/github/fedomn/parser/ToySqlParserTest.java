package com.github.fedomn.parser;

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ToySqlParserTest {

    @Test
    void parse_query_success() throws SqlParseException {
        var node = ToySqlParser.parseQuery("select * from emps where id = 1");
        var expected = """
                SELECT *
                FROM `emps`
                WHERE `id` = 1""";
        assertEquals(expected, node.toSqlString(MysqlSqlDialect.DEFAULT).toString());
    }

    @Test
    void parse_expression_success() throws SqlParseException {
        var node = ToySqlParser.parseExpression("id=1 and name='2'");
        var expected = "`id` = 1 AND `name` = '2'";
        assertEquals(expected, node.toSqlString(MysqlSqlDialect.DEFAULT).toString());
    }
}

