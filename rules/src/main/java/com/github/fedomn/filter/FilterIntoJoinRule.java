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

    // applyRule 核心步骤：
    //  - matchOperands：正如 plan tree，rule.Operand 去 top-down 匹配每一个 node 时，发现 LogicalFilter 满足 rule 条件，从而将匹配到的 nodes 放入 bindings中，这里是 [LogicalFilter, LogicalJoin]
    //  - fireRule：构造 HepRuleCall，并执行 rule onMatch(call) 方法，进行节点的转换，并将结果保存在 call results 中，这里是将 `Filter{Join{Scan, Scan}}` 转换为 `Join{Filter{Scan}, Scan}`
    //  - applyTransformationResults：将 result apply 到 graph 中，并维护 parents 和 children 节点

    // 最后一步 buildFinalPlan，遍历 graph 重新构造 plan tree

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
