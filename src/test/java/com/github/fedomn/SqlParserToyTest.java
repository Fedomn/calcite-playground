package com.github.fedomn;

import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.apache.calcite.config.Lex.MYSQL;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlParserToyTest {

    @Test
    void parse_query_success() throws SqlParseException {
        var config = SqlParser.config().withLex(MYSQL);
        var parser = SqlParser.create("", config);
        var node = parser.parseQuery("select * from emps where id = 1");
        var expected = """
                SELECT *
                FROM `emps`
                WHERE `id` = 1""";
        assertEquals(expected, node.toSqlString(MysqlSqlDialect.DEFAULT).toString());
    }

    @Test
    void parse_expression_success() throws SqlParseException {
        var config = SqlParser.config().withLex(MYSQL);
        var expr = "id=1 and name='2'";
        var parser = SqlParser.create(expr, config);
        var node = parser.parseExpression();
        var expected = "`id` = 1 AND `name` = '2'";
        assertEquals(expected, node.toSqlString(MysqlSqlDialect.DEFAULT).toString());
    }
}

