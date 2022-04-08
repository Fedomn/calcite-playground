/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial;

import com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;
import com.github.zabetak.calcite.tutorial.rules.LuceneTableScanRule;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/**
 * Query processor for running TPC-H queries over Apache Lucene.
 */
public class LuceneQueryProcessor {

  /**
   * Plans and executes an SQL query in the file specified
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: processor SQL_FILE");
      System.exit(-1);
    }
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);

    //-----------------------------step 1: SqlParser and SqlValidator------------------------------------------------

    // TODO 1. Create the root schema and type factory
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // TODO 2. Create the data type for each TPC-H table
    // TODO 3. Add the TPC-H table to the schema
    for (TpchTable table : TpchTable.values()) {
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
      for (TpchTable.Column column : table.columns) {
        RelDataType type = typeFactory.createJavaType(column.type);
        builder.add(column.name, type.getSqlTypeName()).nullable(true);
      }
      String indexPath = DatasetIndexer.INDEX_LOCATION + "/tpch/" + table.name();
      schema.add(table.name(), new LuceneTable(indexPath, builder.build()));
    }

    // TODO 4. Create an SQL parser
    SqlParser parser = SqlParser.create(sqlQuery);
    // TODO 5. Parse the query into an AST
    // TODO 6. Print and check the AST
    SqlNode sqlNode = parser.parseQuery();
    System.out.println("[Parsed query]");
    System.out.println(sqlNode.toString());

    // TODO 7. Configure and instantiate the catalog reader
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList("bs"), typeFactory, config);

    // TODO 8. Create the SQL validator using the standard operator table and default configuration
    SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        catalogReader, typeFactory, SqlValidator.Config.DEFAULT);

    // TODO 9. Validate the initial AST
    SqlNode validNode = validator.validate(sqlNode);
    System.out.println("[Validated query]");
    System.out.println(validNode.toString());

    //-----------------------------step 2: use SqlToRelConverter to generate LogicalPlan------------------------------------------------

    // TODO 10. Create the optimization cluster to maintain planning information
    RelOptPlanner volcanoPlanner = new VolcanoPlanner();
    volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(typeFactory));

    // TODO 11. Configure and instantiate the converter of the AST to Logical plan
    // - No view expansion (use NOOP_EXPANDER)
    RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;
    // - Standard expression normalization (use StandardConvertletTable.INSTANCE)
    // - Default configuration (SqlToRelConverter.config())
    SqlToRelConverter relConverter = new SqlToRelConverter(
        NOOP_EXPANDER,
        validator,
        catalogReader,
        cluster,
        StandardConvertletTable.INSTANCE,
        SqlToRelConverter.config());

    // TODO 12. Convert the valid AST into a logical plan
    RelNode logPlan = relConverter.convertQuery(validNode, false, true).rel;
    // TODO 13. Display the logical plan with explain attributes
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    //-----------------------------step 3: LogicalPlan to PhysicalPlan------------------------------------------------

    // TODO 14. Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(CoreRules.FILTER_INTO_JOIN); // before-cost=4511rows, after-cost=2769rows
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);

    planner.addRule(LuceneTableScanRule.DEFAULT.toRule());

    // TODO 15. Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)
    logPlan = planner.changeTraits(logPlan,
        logPlan.getTraitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logPlan);

    // TODO 16. Start the optimization process to obtain the most efficient physical plan based on
    // the provided rule set.
    EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();
    // TODO 17. Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.ALL_ATTRIBUTES));

    //-----------------------------step 4: PhysicalPlan to ExecutablePlan------------------------------------------------

    // TODO 18. Compile generated code and obtain the executable program
    Bindable<Object[]> executablePlan = EnumerableInterpretable.toBindable(
        new HashMap<>(),
        null,
        phyPlan,
        Prefer.ARRAY);

    long start = System.currentTimeMillis();

    // TODO 19. Run the program using a context simply providing access to the schema and print
    for (Object[] row : executablePlan.bind(new SchemaOnlyDataContext(schema))) {
      System.out.println(Arrays.toString(row));
    }

    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  /**
   * A simple data context only with schema information.
   */
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
