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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsScan;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSort;
import static org.hamcrest.CoreMatchers.not;

/**
 * Test Subquery rewrite rule.
 */
public class SubqueryRewriteRuleTest extends GridCommonAbstractTest {
    /** */
    public static final String IDX_ORDER_CUST_KEY = "IDX_ORDER_CUST_KEY";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(2);

        IgniteCache<Integer, Customer> custCache;
        IgniteCache<Integer, Order> ordCache;
        IgniteCache<Integer, Supplier> supCache;
        {
            QueryEntity qryEnt = new QueryEntity();
            qryEnt.setTableName("customer");
            qryEnt.setKeyFieldName("c_custkey");
            qryEnt.setKeyType(Integer.class.getName());
            qryEnt.setValueType(Customer.class.getName());

            qryEnt.addQueryField("c_custkey", Integer.class.getName(), null);
            qryEnt.addQueryField("countryKey", Integer.class.getName(), "c_countrykey");
            qryEnt.addQueryField("name", String.class.getName(), "c_name");

            CacheConfiguration<Integer, Customer> ccfg = new CacheConfiguration<>(qryEnt.getTableName());
            ccfg.setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setQueryEntities(singletonList(qryEnt))
                    .setSqlSchema("PUBLIC");

            custCache = grid.createCache(ccfg);
        }

        {
            QueryEntity qryEnt = new QueryEntity();
            qryEnt.setTableName("orders");
            qryEnt.setKeyFieldName("o_orderkey");
            qryEnt.setKeyType(Integer.class.getName());
            qryEnt.setValueType(Order.class.getName());

            qryEnt.addQueryField("o_orderkey", Integer.class.getName(), null);
            qryEnt.addQueryField("custId", Integer.class.getName(), "o_custkey");
            qryEnt.addQueryField("totalPrice", Long.class.getName(), "o_totalprice");

            qryEnt.setIndexes(asList(
                    new QueryIndex("custId", QueryIndexType.SORTED).setName(IDX_ORDER_CUST_KEY)
            ));

            CacheConfiguration<Integer, Order> ccfg = new CacheConfiguration<>(qryEnt.getTableName());
            ccfg.setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setQueryEntities(singletonList(qryEnt))
                    .setSqlSchema("PUBLIC");

            ordCache = grid.createCache(ccfg);
        }

        {
            QueryEntity qryEnt = new QueryEntity();
            qryEnt.setTableName("supplier");
            qryEnt.setKeyFieldName("s_supkey");
            qryEnt.setKeyType(Integer.class.getName());
            qryEnt.setValueType(Order.class.getName());

            qryEnt.addQueryField("s_supkey", Integer.class.getName(), null);
            qryEnt.addQueryField("countryKey", Integer.class.getName(), "s_countrykey");

            CacheConfiguration<Integer, Supplier> ccfg = new CacheConfiguration<>(qryEnt.getTableName());
            ccfg.setCacheMode(CacheMode.PARTITIONED)
                    .setBackups(1)
                    .setQueryEntities(singletonList(qryEnt))
                    .setSqlSchema("PUBLIC");

            supCache = grid.createCache(ccfg);
        }

        awaitPartitionMapExchange();
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
            "    SELECT * FROM ORDERS\n" +
            "    WHERE o_custkey = c_custkey \n" + // Correlated.
            ")")
            .and(QueryChecker.containsJoin("semi"))
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
    public void testNonInToAntiJoinRule() throws Exception {
        checkQuery("SELECT c_name\n" +
            "FROM CUSTOMER\n" +
            "WHERE c_countrykey <> ALL (SELECT s_countrykey FROM SUPPLIER)")
            .and(QueryChecker.containsJoin("anti"))
            .and(containsScan("PUBLIC", "CUSTOMER", "PK"))
            .and(containsScan("PUBLIC", "ORDERS", "PK"))
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
        };
    }

    /**
     *
     */
    static class Customer {
        /** */
        int id;

        /** */
        String name;

        /** */
        int countrykey;
    }

    /**
     *
     */
    static class Order {
        /** */
        int id;

        /** */
        long totalPrice;

        /** */
        @AffinityKeyMapped
        int custId;
    }

    /**
     *
     */
    static class Supplier {
        /** */
        int id;

        /** */
        int countrykey;
    }
}
