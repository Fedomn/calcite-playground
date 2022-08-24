package com.github.fedomn.calc;

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParseException;

import static com.github.fedomn.Tester.printOptimizeBeforeAfter;

public class CalcMergeRule {
  public static void main(String[] args) throws SqlParseException {

    String sql = "select * from t_users where id = 1";
    printOptimizeBeforeAfter(sql,
        CoreRules.FILTER_TO_CALC,
        CoreRules.PROJECT_TO_CALC,
        CoreRules.CALC_MERGE);

    /*
    Before:
    LogicalProject(id=[$0], name=[$1], role_id=[$2])
      LogicalFilter(condition=[=($0, 1)])
        LogicalTableScan(table=[[t_users]])

    After:
    LogicalCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[=($t0, $t3)], proj#0..2=[{exprs}], $condition=[$t4])
      LogicalTableScan(table=[[t_users]])
     */
  }
}
