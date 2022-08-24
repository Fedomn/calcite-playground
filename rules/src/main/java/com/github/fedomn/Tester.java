package com.github.fedomn;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class Tester {

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

  private static RelOptCluster newCluster(JavaTypeFactoryImpl typeFactory) {
    var planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  public static void printOptimizeBeforeAfter(String sqlQuery, RelOptRule... rules) throws SqlParseException {
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
    rolesType.add("role_name", SqlTypeName.VARCHAR);
    var rolesTable = new MemTable(rolesType.build(), ROLES_DATA);
    schema.add("t_roles", rolesTable);

    // 4. Create SQL parser
    var sqlParser = SqlParser.create(sqlQuery);
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


    // Optimizer Part
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    for (final RelOptRule rule : rules) {
      hepProgramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN).addRuleInstance(rule);
    }
    HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build());
    hepPlanner.setRoot(logicalPlan);
    RelNode bestExp = hepPlanner.findBestExp();

    System.out.printf("""
        Before: \n%s
        After: \n%s
        """, RelOptUtil.toString(logicalPlan), RelOptUtil.toString(bestExp));

  }
}


