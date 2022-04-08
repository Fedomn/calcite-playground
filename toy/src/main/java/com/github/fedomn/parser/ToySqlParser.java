package com.github.fedomn.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import static org.apache.calcite.config.Lex.MYSQL;

public abstract class ToySqlParser {

    private static SqlParser.Config config() {
        return SqlParser
                .config()
                .withCaseSensitive(false)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withLex(MYSQL);
    }

    public static SqlNode parseQuery(String sql) throws SqlParseException {
        var config = config();
        var parser = SqlParser.create(sql, config);
        return parser.parseStmt();
    }

    public static SqlNode parseExpression(String expr) throws SqlParseException {
        var config = config();
        var parser = SqlParser.create(expr, config);
        return parser.parseExpression();
    }
}