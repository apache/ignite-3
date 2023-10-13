/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.benchmarks;

import java.util.List;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.ClusterBuilder;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * Provides utility methods to register tables described by the TPC-H benchmark in a {@link ClusterBuilder}.
 */
public final class TpchSchema {

    private static final int PART_SIZE = 200_000;
    private static final int SUPPLIER_SIZE = 10_000;
    // TPC-H 4.2.5.1 Table 3: Estimated Database Size
    private static final int PARTSUPP_SIZE = 80 * SUPPLIER_SIZE;
    private static final int ORDERS_SIZE = 1_500_000;
    // TPC-H 4.2.5.1 Table 3: Estimated Database Size
    private static final int LINEITEM_SIZE = 4 * ORDERS_SIZE;
    private static final int CUSTOMER_SIZE = 150_000;
    private static final int NATION_SIZE = 25;
    private static final int REGION_SIZE = 5;

    private TpchSchema() {

    }

    /**
     * Registers tables from the TPC-H benchmark in the given {@link ClusterBuilder cluster builder} with the scaling factor of {@code 1}.
     *
     * @param clusterBuilder  A cluster builder.
     * @param dataSize The number of rows data provider is going to produce for each table.
     */
    public static void registerTables(ClusterBuilder clusterBuilder, int dataSize) {
        registerTables(clusterBuilder, 1, dataSize);
    }

    /**
     * Registers tables from the TPC-H benchmark in the given cluster with the given scaling factor.
     *
     * @param clusterBuilder  A cluster builder.
     * @param scalingFactor Scaling factor.
     * @param dataSize The number of rows data provider is going to produce for each table.
     */
    public static void registerTables(ClusterBuilder clusterBuilder, int scalingFactor, int dataSize) {
        // Register default data provider factory that is going to generate pseudo random data data.
        clusterBuilder.defaultDataProviderFactory(new RepeatedRandomRowDataProviderFactory(dataSize));

        clusterBuilder.addTable().name("PART")
                .addColumn("P_PARTKEY", NativeTypes.INT64)
                .addColumn("P_NAME", NativeTypes.stringOf(55))
                .addColumn("P_MFGR", NativeTypes.stringOf(25))
                .addColumn("P_BRAND", NativeTypes.stringOf(10))
                .addColumn("P_TYPE", NativeTypes.stringOf(25))
                .addColumn("P_SIZE", NativeTypes.INT32)
                .addColumn("P_CONTAINER", NativeTypes.stringOf(10))
                .addColumn("P_RETAILPRICE", NativeTypes.decimalOf(15, 2))
                .addColumn("P_COMMENT", NativeTypes.stringOf(23))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(scalingFactor * PART_SIZE).end();

        clusterBuilder.addTable().name("SUPPLIER")
                .addColumn("S_SUPPKEY", NativeTypes.INT64)
                .addColumn("S_NAME", NativeTypes.stringOf(25))
                .addColumn("S_ADDRESS", NativeTypes.stringOf(40))
                .addColumn("S_NATIONKEY", NativeTypes.INT64)
                .addColumn("S_PHONE", NativeTypes.stringOf(15))
                .addColumn("S_ACCTBAL", NativeTypes.decimalOf(15, 2))
                .addColumn("S_COMMENT", NativeTypes.stringOf(101))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(scalingFactor * SUPPLIER_SIZE).end();

        clusterBuilder.addTable().name("PARTSUPP")
                .addColumn("PS_PARTKEY", NativeTypes.INT64)
                .addColumn("PS_SUPPKEY", NativeTypes.INT64)
                .addColumn("PS_AVAILQTY", NativeTypes.INT32)
                .addColumn("PS_SUPPLYCOST", NativeTypes.decimalOf(15, 2))
                .addColumn("PS_COMMENT", NativeTypes.stringOf(199))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(scalingFactor * PARTSUPP_SIZE).end();

        clusterBuilder.addTable().name("CUSTOMER")
                .addColumn("C_CUSTKEY", NativeTypes.INT64)
                .addColumn("C_NAME", NativeTypes.stringOf(25))
                .addColumn("C_ADDRESS", NativeTypes.stringOf(40))
                .addColumn("C_NATIONKEY", NativeTypes.INT64)
                .addColumn("C_PHONE", NativeTypes.stringOf(15))
                .addColumn("C_ACCTBAL", NativeTypes.decimalOf(15, 2))
                .addColumn("C_MKTSEGMENT", NativeTypes.stringOf(10))
                .addColumn("C_COMMENT", NativeTypes.stringOf(117))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(scalingFactor * CUSTOMER_SIZE)
                .end();

        clusterBuilder.addTable().name("ORDERS")
                .addColumn("O_ORDERKEY", NativeTypes.INT64)
                .addColumn("O_CUSTKEY", NativeTypes.INT64)
                .addColumn("O_ORDERSTATUS", NativeTypes.stringOf(1))
                .addColumn("O_TOTALPRICE", NativeTypes.decimalOf(15, 2))
                .addColumn("O_ORDERDATE", NativeTypes.datetime(6))
                .addColumn("O_ORDERPRIORITY", NativeTypes.stringOf(15))
                .addColumn("O_CLERK", NativeTypes.stringOf(15))
                .addColumn("O_SHIPPRIORITY", NativeTypes.INT32)
                .addColumn("O_COMMENT", NativeTypes.stringOf(79))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(scalingFactor * ORDERS_SIZE)
                .end();

        clusterBuilder.addTable().name("LINEITEM")
                .addColumn("L_ORDERKEY", NativeTypes.INT64)
                .addColumn("L_PARTKEY", NativeTypes.INT64)
                .addColumn("L_SUPPKEY", NativeTypes.INT64)
                .addColumn("L_LINENUMBER", NativeTypes.INT32)
                .addColumn("L_QUANTITY", NativeTypes.decimalOf(15, 2))
                .addColumn("L_EXTENDEDPRICE", NativeTypes.decimalOf(15, 2))
                .addColumn("L_DISCOUNT", NativeTypes.decimalOf(15, 2))
                .addColumn("L_TAX", NativeTypes.decimalOf(15, 2))
                .addColumn("L_RETURNFLAG", NativeTypes.stringOf(1))
                .addColumn("L_LINESTATUS", NativeTypes.stringOf(1))
                .addColumn("L_SHIPDATE", NativeTypes.datetime(6))
                .addColumn("L_COMMITDATE", NativeTypes.datetime(6))
                .addColumn("L_RECEIPTDATE", NativeTypes.datetime(6))
                .addColumn("L_SHIPINSTRUCT", NativeTypes.stringOf(25))
                .addColumn("L_SHIPMODE", NativeTypes.stringOf(10))
                .addColumn("L_COMMENT", NativeTypes.stringOf(44))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(scalingFactor * LINEITEM_SIZE)
                .end();

        clusterBuilder.addTable().name("NATION")
                .addColumn("N_NATIONKEY", NativeTypes.INT64)
                .addColumn("N_NAME", NativeTypes.stringOf(25))
                .addColumn("N_REGIONKEY", NativeTypes.INT64)
                .addColumn("N_COMMENT", NativeTypes.stringOf(152))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(NATION_SIZE)
                .end();

        clusterBuilder.addTable().name("REGION")
                .addColumn("R_REGIONKEY", NativeTypes.INT64)
                .addColumn("R_NAME", NativeTypes.stringOf(25))
                .addColumn("N_COMMENT", NativeTypes.stringOf(152))
                .distribution(IgniteDistributions.hash(List.of(0)))
                .size(REGION_SIZE)
                .end();

    }
}
