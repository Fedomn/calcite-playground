package com.github.fedomn.e2e;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ToyQueryProcessorEnumerableTest {
    private static final List<Object[]> USERS_DATA = asList(
            new Object[]{1, "u1", 1},
            new Object[]{2, "u2", 2},
            new Object[]{3, "u3", 3}
    );

    private static final List<Object[]> ROLES_DATA = asList(
            new Object[]{1, "r1"},
            new Object[]{2, "r2"},
            new Object[]{3, "r3"}
    );


    @Test
    void main_test() throws SqlParseException {
        // 1. Instantiate a type factory for creating types (e.g., VARCHAR, NUMERIC, etc.)
        var typeFactory = new JavaTypeFactoryImpl();

        // 2. Create the root schema describing the data model
        var schema = CalciteSchema.createRootSchema(true);

        // 3. Define type for users and roles table
        var usersType = new RelDataTypeFactory.Builder(typeFactory);
        usersType.add("id", SqlTypeName.INTEGER);
        usersType.add("name", SqlTypeName.VARCHAR);
        usersType.add("role_id", SqlTypeName.INTEGER);
        var usersTable = new MemTable(usersType.build(), USERS_DATA);
        schema.add("t_users", usersTable);

        var rolesType = new RelDataTypeFactory.Builder(typeFactory);
        rolesType.add("id", SqlTypeName.INTEGER);
        rolesType.add("name", SqlTypeName.VARCHAR);
        var rolesTable = new MemTable(rolesType.build(), ROLES_DATA);
        schema.add("t_roles", rolesTable);

        // 4. Create SQL parser
        var sqlParser = SqlParser.create("""
                select u.* from t_users u
                inner join t_roles r on u.role_id = r.id
                where r.id >= 2
                order by r.id desc
                limit 10
                """);
        // 5. Parse query into AST
        var sqlNode = sqlParser.parseQuery();

        // 6. Configure and instantiate validator
        var props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        var config = new CalciteConnectionConfigImpl(props);
        var catalogReader = new CalciteCatalogReader(schema, emptyList(), typeFactory, config);
        var validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT
        );

        // 7. Validate the initial AST
        var validNode = validator.validate(sqlNode);

        // 8. Configure and instantiate the converter of the AST to Logical plan (requires opt cluster)
        var cluster = newCluster(typeFactory);

        RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;
        var relConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config()
        );

        // 9. Convert the valid AST into a logical plan
        RelNode logicalPlan = relConverter.convertQuery(validNode, false, true).rel;
        // Display the logical plan
        String logicalPlanExplainText = RelOptUtil.dumpPlan("[Logical plan]", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
        assertEquals("""
                [Logical plan]
                LogicalSort(sort0=[$3], dir0=[DESC], fetch=[10]), id = 8
                  LogicalProject(id=[$0], name=[$1], role_id=[$2], id0=[$3]), id = 7
                    LogicalFilter(condition=[>=($3, 2)]), id = 6
                      LogicalJoin(condition=[=($2, $3)], joinType=[inner]), id = 5
                        LogicalTableScan(table=[[t_users]]), id = 1
                        LogicalTableScan(table=[[t_roles]]), id = 3
                """, logicalPlanExplainText);

        // 10. Initialize optimizer/planner with the necessary rules
        var planner = cluster.getPlanner();
        // transformation rules
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(CoreRules.FILTER_TO_CALC);
        planner.addRule(CoreRules.PROJECT_TO_CALC);
        // implementation rules (not used project_rules reason: EnumerableProject implement UnsupportedOperationException)
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

        // 11. Define the type of the output plan (in this case we want a physical plan in EnumerableConvention)
        logicalPlan = planner.changeTraits(logicalPlan, cluster.traitSet().replace(EnumerableConvention.INSTANCE));
        planner.setRoot(logicalPlan);

        // 12. Start the optimization process to obtain the most efficient physical plan based on the provided rule set.
        var physicalPlan = (EnumerableRel) planner.findBestExp();
        // Display the physical plan
        String physicalPlanExplainText = RelOptUtil.dumpPlan("[Physical plan]", physicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES);
        assertEquals("""
                [Physical plan]
                EnumerableLimitSort(sort0=[$3], dir0=[DESC], fetch=[10]), id = 45
                  EnumerableCalc(expr#0..4=[{inputs}], proj#0..3=[{exprs}]), id = 44
                    EnumerableHashJoin(condition=[=($2, $3)], joinType=[inner]), id = 43
                      EnumerableTableScan(table=[[t_users]]), id = 21
                      EnumerableCalc(expr#0..1=[{inputs}], expr#2=[2], expr#3=[>=($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3]), id = 42
                        EnumerableTableScan(table=[[t_roles]]), id = 23
                """, physicalPlanExplainText);

        // 13. Run the executable plan using a context simply providing access to the schema
        var expectedRows = asList(
                new Object[]{3, "u3", 3, 3},
                new Object[]{2, "u2", 2, 2}
        ).iterator();
        Bindable<Object[]> executablePlan = EnumerableInterpretable.toBindable(
                new HashMap<>(),
                null,
                physicalPlan,
                EnumerableRel.Prefer.ARRAY
        );
        for (Object[] row : executablePlan.bind(new SchemaOnlyDataContext(schema))) {
            assertArrayEquals(expectedRows.next(), row);
        }
    }

    private static RelOptCluster newCluster(JavaTypeFactoryImpl typeFactory) {
        var planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    private static class MemTable extends AbstractTable implements ScannableTable {
        private final RelDataType rowType;
        private final List<Object[]> data;

        private MemTable(RelDataType rowType, List<Object[]> data) {
            this.rowType = rowType;
            this.data = data;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(data);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType;
        }
    }

    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            return null;
        }
    }
}