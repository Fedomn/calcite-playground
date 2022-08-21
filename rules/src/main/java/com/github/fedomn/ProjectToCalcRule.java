package com.github.fedomn;

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParseException;

import static com.github.fedomn.Tester.printOptimizeBeforeAfter;

// in sql-query-engine-rs pattern: BoundExpr to InputRef
public class ProjectToCalcRule {
  public static void main(String[] args) throws SqlParseException {

    String sql = "select * from t_users";
    printOptimizeBeforeAfter(sql, CoreRules.PROJECT_TO_CALC);

    /*
    Before:
    LogicalProject(id=[$0], name=[$1], role_id=[$2])
      LogicalTableScan(table=[[t_users]])

    After:
    LogicalCalc(expr#0..2=[{inputs}], proj#0..2=[{exprs}])
      LogicalTableScan(table=[[t_users]])
    */
  }
}
