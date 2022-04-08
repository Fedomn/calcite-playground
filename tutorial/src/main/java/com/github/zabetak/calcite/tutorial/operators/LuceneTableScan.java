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
package com.github.zabetak.calcite.tutorial.operators;

import com.github.zabetak.calcite.tutorial.LuceneTable;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.lucene.search.MatchAllDocsQuery;

/**
 * Implementation of {@link TableScan} in {@link LuceneRel#LUCENE} convention.
 * <p>
 * The expression knows where is the Lucene index located and how to access it.
 */
public final class LuceneTableScan extends TableScan implements LuceneRel {

  // TODO 1. Extend TableScan operator
  // TODO 2. Implement LuceneRel interface
  // TODO 3. Implement missing methods
  public LuceneTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
      RelOptTable table) {
    super(cluster, traitSet, hints, table);
  }

  @Override
  public Result implement() {
    LuceneTable luceneTable = getTable().unwrap(LuceneTable.class);
    return new Result(luceneTable.indexPath(), new MatchAllDocsQuery());
  }
}
