/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rules;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.calcite.tools.Frameworks.newConfigBuilder;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSort;
import static org.apache.ignite.internal.processors.query.calcite.schema.IgniteTableImpl.PK_INDEX_NAME;
import static org.hamcrest.CoreMatchers.not;

/**
 * Test Subquery rewrite rule.
 */
public class SubqueryRewriteRuleTest extends GridCommonAbstractTest {
    /** */
    private List<UUID> nodes;

    /** */
    private PlanningContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        nodes = new ArrayList<>(4);

        for (int i = 0; i < 4; i++)
            nodes.add(UUID.randomUUID());

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        TestTable customer = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("C_CUSTKEY", f.createJavaType(Integer.class))
                        .add("C_NAME", f.createJavaType(String.class))
                        .add("C_COUNTRYKEY", f.createJavaType(Integer.class))
                        .build());

        TestTable orders = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("O_ORDERKEY", f.createJavaType(Integer.class))
                        .add("O_CUSTKEY", f.createJavaType(Integer.class))
                        .add("O_TOTALPRICE", f.createJavaType(Integer.class))
                        .build());

        orders.addIndex(new IgniteIndex(RelCollations.of(1), "IDX_O_CUSTKEY", null, orders));

        TestTable suppliers = new TestTable(
                new RelDataTypeFactory.Builder(f)
                        .add("S_SUPKEY", f.createJavaType(Integer.class))
                        .add("S_COUNTRYKEY", f.createJavaType(Integer.class))
                        .build());

        suppliers.addIndex(new IgniteIndex(RelCollations.of(1), "IDX_S_COUNTRYKEY", null, suppliers));

        IgniteSchema publicSchema = new IgniteSchema("PUBLIC");

        publicSchema.addTable("CUSTOMER", customer);
        publicSchema.addTable("ORDERS", orders);
        publicSchema.addTable("SUPPLIER", suppliers);

        SchemaPlus schema = createRootSchema(false)
                .add("PUBLIC", publicSchema);

        RelTraitDef<?>[] traitDefs = {
                DistributionTraitDef.INSTANCE,
                ConventionTraitDef.INSTANCE,
                RelCollationTraitDef.INSTANCE
        };

        ctx = PlanningContext.builder()
                .localNodeId(F.first(nodes))
                .originatingNodeId(F.first(nodes))
                .parentContext(Contexts.empty())
                .frameworkConfig(newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(schema)
                        .traitDefs(traitDefs)
                        .build())
                .logger(log)
                .parameters(2)
                .topologyVersion(AffinityTopologyVersion.NONE)
                .build();
    }

    /**
     * Check subquery rewrite rule is not applied for non-correlated query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonCorrelatedQuery() throws Exception {
        checkQuery("SELECT c_count, count(*) AS custdist\n" +
            "FROM (\n" +
            "     SELECT c_custkey, count(o_orderkey) AS c_count\n" +
            "     FROM CUSTOMER\n" +
            "     LEFT OUTER JOIN ORDERS ON c_custkey = o_custkey\n" +
            "     GROUP BY c_custkey\n" +
            "     ) c_orders\n" +
            "GROUP BY c_count\n" +
            "ORDER BY custdist DESC, c_count DESC;")
            .and(QueryChecker.containsJoin("left"))
            .and(containsSort())
            .and(not(QueryChecker.containsJoin("inner")))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "ORDERS", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for correlated query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScalarValueInWhereClause() throws Exception {
        checkQuery("SELECT c_custkey\n" +
            "FROM CUSTOMER\n" +
            "WHERE 1000000 < (\n" +
            "    SELECT SUM(o_totalprice)\n" +
            "    FROM ORDERS\n" +
            "    WHERE o_custkey = c_custkey\n" + // Correlated.
            ")")
            .and(QueryChecker.containsJoin("inner"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "ORDERS", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for correlated query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testScalarValueInSelectClause() throws Exception {
        checkQuery("SELECT o_orderkey, (\n" +
            "    SELECT c_name\n" +
            "    FROM CUSTOMER\n" +
            "    WHERE c_custkey = o_custkey \n" + // Correlated.
            ") AS c_name FROM ORDERS")
            .and(QueryChecker.containsJoin("left"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "ORDERS", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for correlated query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExistsToSemiJoinRule() throws Exception {
        checkQuery("SELECT c_custkey\n" +
            "FROM CUSTOMER\n" +
            "WHERE c_countrykey = 86 AND EXISTS(\n" +
            "    SELECT 1 FROM ORDERS\n" +
            "    WHERE o_custkey = c_custkey \n" + // Correlated.
            ")")
            .and(QueryChecker.containsJoin("semi"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "ORDERS", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for non-correlated query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonInToSemiJoinRule() throws Exception {
        checkQuery("SELECT c_name\n" +
            "FROM CUSTOMER\n" +
            "WHERE c_countrykey IN (SELECT s_countrykey FROM SUPPLIER)")
            .and(QueryChecker.containsJoin("semi"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "SUPPLIER", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for correlated query.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13159")
    @Test
    public void testNotExistsToAntiJoinRule() throws Exception {
        checkQuery("SELECT c_custkey\n" +
            "FROM CUSTOMER\n" +
            "WHERE NOT EXISTS(\n" +
            "    SELECT 1 FROM ORDERS\n" +
            "    WHERE o_custkey = c_custkey \n" + // Correlated.
            ")")
            .and(QueryChecker.containsJoin("anti"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "ORDERS", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for non-correlated query.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13159")
    @Test
    public void testNonAllToSemiAntiJoinRule() throws Exception {
        checkQuery("SELECT c_name\n" +
            "FROM CUSTOMER\n" +
            "WHERE c_countrykey <> ALL (SELECT s_countrykey FROM SUPPLIER)")
            .and(QueryChecker.containsJoin("anti"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "SUPPLIER", "PK"))
            .check();
    }

    /**
     * Check subquery rewrite rule is applied for correlated query.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-13159")
    @Test
    public void testNonInToSemiAntiJoinRule2() throws Exception {
        checkQuery("SELECT c_name\n" +
            "FROM CUSTOMER\n" +
            "WHERE c_countrykey NOT IN (SELECT s_countrykey FROM SUPPLIER WHERE s_supkey = c_custkey)")
            .and(QueryChecker.containsJoin("left"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "SUPPLIER", "PK"))
            .check();
    }

    /**
     *
     */
    private QueryChecker checkQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(grid(0).context(), QueryEngine.class);
            }

            @Override public void check() {
                RelNode root;

                try (IgnitePlanner planner = ctx.planner()) {
                    assertNotNull(planner);

                    assertNotNull(qry);

                    // Parse
                    SqlNode sqlNode = planner.parse(qry);

                    // Validate
                    sqlNode = planner.validate(sqlNode);

                    // Convert to Relational operators graph
                    root = planner.convert(sqlNode);

                    // Transformation chain
                    root = planner.transform(PlannerPhase.HEURISTIC_OPTIMIZATION, root.getTraitSet(), root);

                    // Transformation chain
                    RelTraitSet desired = root.getCluster().traitSet()
                            .replace(IgniteConvention.INSTANCE)
                            .replace(IgniteDistributions.single())
                            .replace(planner.rel(sqlNode).collation == null ? RelCollations.EMPTY : planner.rel(sqlNode).collation)
                            .simplify();

                    root = planner.transform(PlannerPhase.OPTIMIZATION, desired, root);

                    String queryPlan = RelOptUtil.toString(root);

                    log.info("Actual plan: " + queryPlan);

                    validatePlan(queryPlan);
                } catch (Exception e) {
                    log.error("Failed to build query plan:", e);

                    fail(e.getMessage());
                }
            }
        };
    }

    /** */
    private static class TestTable implements IgniteTable {
        /** */
        private final RelProtoDataType protoType;

        /** */
        private final Map<String, IgniteIndex> indexes = new HashMap<>();

        /** */
        private TestTable(RelDataType type) {
            protoType = RelDataTypeImpl.proto(type);

            addIndex(new IgniteIndex(RelCollations.EMPTY, PK_INDEX_NAME, null, this));
        }

        /** {@inheritDoc} */
        @Override public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable relOptTbl) {
            RelOptCluster cluster = ctx.getCluster();

            return toRel(cluster, relOptTbl, PK_INDEX_NAME);
        }

        /** {@inheritDoc} */
        @Override public IgniteTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
            RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replaceIfs(RelCollationTraitDef.INSTANCE, this::collations)
                    .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

            IgniteIndex idx = getIndex(idxName);

            if (idx == null)
                return null;

            traitSet = traitSet.replace(idx.collation());

            return new IgniteTableScan(cluster, traitSet, relOptTbl, idxName, null);
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoType.apply(typeFactory);
        }

        /** {@inheritDoc} */
        @Override public Statistic getStatistic() {
            return new Statistic() {
                /** {@inheritDoc */
                @Override public Double getRowCount() {
                    return 100.0;
                }

                /** {@inheritDoc */
                @Override public boolean isKey(ImmutableBitSet cols) {
                    return false;
                }

                /** {@inheritDoc */
                @Override public List<ImmutableBitSet> getKeys() {
                    throw new AssertionError();
                }

                /** {@inheritDoc */
                @Override public List<RelReferentialConstraint> getReferentialConstraints() {
                    throw new AssertionError();
                }

                /** {@inheritDoc */
                @Override public List<RelCollation> getCollations() {
                    return Collections.emptyList();
                }

                /** {@inheritDoc */
                @Override public RelDistribution getDistribution() {
                    throw new AssertionError();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
            throw new AssertionError();
        }


        /** {@inheritDoc} */
        @Override public Schema.TableType getJdbcTableType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public boolean isRolledUp(String col) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
                                                              CalciteConnectionConfig config) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public NodesMapping mapping(PlanningContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            return IgniteDistributions.broadcast();
        }

        /** {@inheritDoc} */
        @Override public List<RelCollation> collations() {
            return ImmutableList.of(RelCollations.EMPTY);
        }

        /** {@inheritDoc} */
        @Override public TableDescriptor descriptor() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Map<String, IgniteIndex> indexes() {
            return indexes;
        }

        /** {@inheritDoc} */
        @Override public void addIndex(IgniteIndex idxTbl) {
            indexes.put(idxTbl.name(), idxTbl);
        }

        /** {@inheritDoc} */
        @Override public IgniteIndex getIndex(String idxName) {
            return indexes.get(idxName);
        }

        /** {@inheritDoc} */
        @Override public void removeIndex(String idxName) {
            throw new AssertionError();
        }
    }
}
