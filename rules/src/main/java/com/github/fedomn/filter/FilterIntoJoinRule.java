package com.github.fedomn.filter;

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParseException;

import static com.github.fedomn.Tester.printOptimizeBeforeAfter;

public class FilterIntoJoinRule {
  public static void main(String[] args) throws SqlParseException {

    String sql = """
        select name, role_name from t_users a left join t_roles b on a.role_id = b.id
        where a.id = 1
        """;
    printOptimizeBeforeAfter(sql, CoreRules.FILTER_INTO_JOIN);

    /*
    Before:
    LogicalProject(NAME=[$1], ROLE_NAME=[$4])
      LogicalFilter(condition=[=($0, 1)])
        LogicalJoin(condition=[=($2, $3)], joinType=[left])
          LogicalTableScan(table=[[t_users]])
          LogicalTableScan(table=[[t_roles]])

    After:
    LogicalProject(NAME=[$1], ROLE_NAME=[$4])
      LogicalJoin(condition=[=($2, $3)], joinType=[left])
        LogicalFilter(condition=[=($0, 1)])
          LogicalTableScan(table=[[t_users]])
        LogicalTableScan(table=[[t_roles]])
     */
  }
}
