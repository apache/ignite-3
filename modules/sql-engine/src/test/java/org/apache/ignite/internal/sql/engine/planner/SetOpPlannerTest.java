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

package org.apache.ignite.internal.sql.engine.planner;

import java.util.List;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteColocatedIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteColocatedMinus;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteColocatedSetOp;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapSetOp;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceSetOp;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test to verify set op (EXCEPT, INTERSECT).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetOpPlannerTest extends AbstractPlannerTest {
    /** Public schema. */
    private IgniteSchema publicSchema;

    /**
     * Setup.
     *
     * <p>Prepares multiple test tables with different distributions.
     */
    @BeforeAll
    public void setup() {
        publicSchema = new IgniteSchema("PUBLIC");

        IgniteTypeFactory f = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

        RelDataType type = new RelDataTypeFactory.Builder(f)
                .add("ID", f.createJavaType(Integer.class))
                .add("NAME", f.createJavaType(String.class))
                .add("SALARY", f.createJavaType(Double.class))
                .build();

        createTable(publicSchema, "RANDOM_TBL1", type, IgniteDistributions.random());
        createTable(publicSchema, "RANDOM_TBL2", type, IgniteDistributions.random());
        createTable(publicSchema, "BROADCAST_TBL1", type, IgniteDistributions.broadcast());
        createTable(publicSchema, "BROADCAST_TBL2", type, IgniteDistributions.broadcast());
        createTable(publicSchema, "SINGLE_TBL1", type, IgniteDistributions.single());
        createTable(publicSchema, "SINGLE_TBL2", type, IgniteDistributions.single());

        createTable(publicSchema, "AFFINITY_TBL1", type,
                // TODO https://issues.apache.org/jira/browse/IGNITE-18211
                // IgniteDistributions.affinity(0, "Test1", "hash"));
                IgniteDistributions.hash(List.of(0)));


        createTable(publicSchema, "AFFINITY_TBL2", type,
                // TODO https://issues.apache.org/jira/browse/IGNITE-18211
                // IgniteDistributions.affinity(0, "Test2", "hash"));
                IgniteDistributions.hash(List.of(0)));

        createTable(publicSchema, "AFFINITY_TBL3", type,
                IgniteDistributions.affinity(1, "Test3", "hash"));

        createTable(publicSchema, "AFFINITY_TBL4", type,
                IgniteDistributions.affinity(0, "Test4", "hash2"));
    }

    /**
     * Tests SET operations on two tables with random distribution.
     *
     * <p>{@link Type#RANDOM_DISTRIBUTED Random} distribution cannot be colocated
     * with other random distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM random_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(n -> !n.all())
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("random_tbl1")))
                        .and(input(1, isTableScan("random_tbl2")))
                ))
        );
    }

    /**
     * Tests SET operations (with ALL flag enabled) on two tables with random distribution.
     *
     * <p>{@link Type#RANDOM_DISTRIBUTED Random} distribution cannot be colocated
     * with other random distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAllRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM random_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(IgniteSetOp::all)
                        .and(hasChildThat(isInstanceOf(setOp.map)
                                .and(input(0, isTableScan("random_tbl1")))
                                .and(input(1, isTableScan("random_tbl2")))
                        )));
    }

    /**
     * Tests SET operations on two tables with broadcast distribution.
     *
     * <p>The operation is considered colocated because {@link Type#BROADCAST_DISTRIBUTED broadcast}
     * distribution satisfies any other distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpBroadcast(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM broadcast_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                .and(input(0, isTableScan("broadcast_tbl1")))
                .and(input(1, isTableScan("broadcast_tbl2")))
        );
    }

    /**
     * Tests SET operations on two tables with single distribution.
     *
     * <p>The operation is considered colocated because {@link Type#SINGLETON single} distribution
     * satisfies other single distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingle(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM single_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, isTableScan("single_tbl2"))));
    }

    /**
     * Tests SET operations on two tables with single and random distribution.
     *
     * <p>{@link Type#SINGLETON Single} distribution cannot be colocated
     * with {@link Type#RANDOM_DISTRIBUTED random} distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingleAndRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM random_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                .and(hasDistribution(IgniteDistributions.single()))
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, hasChildThat(isTableScan("random_tbl1")))));
    }

    /**
     * Tests SET operations on two tables with single and affinity distribution.
     *
     * <p>{@link Type#SINGLETON Single} distribution cannot be colocated with affinity distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingleAndAffinity(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                .and(hasDistribution(IgniteDistributions.single()))
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, hasChildThat(isTableScan("affinity_tbl1")))));
    }

    /**
     * Tests SET operations on two tables with single and broadcast distribution.
     *
     * <p>The operation is considered colocated because {@link Type#BROADCAST_DISTRIBUTED broadcast}
     * distribution satisfies any other distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpSingleAndBroadcast(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM single_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                .and(input(0, isTableScan("single_tbl1")))
                .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }


    /**
     * Tests SET operations on tables with the same affinity distribution.
     *
     * <p>The operation is considered colocated because the tables are
     * compared against the corresponding collocation columns.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAffinity(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
                .and(input(isInstanceOf(setOp.colocated)
                        // TODO https://issues.apache.org/jira/browse/IGNITE-18211
                        // .and(hasDistribution(IgniteDistributions.affinity(0, null, "hash")))
                        .and(input(0, isTableScan("affinity_tbl1")))
                        .and(input(1, isTableScan("affinity_tbl2")))
                ))
        );
    }

    /**
     * Tests SET operations on two tables with affinity and broadcast distribution.
     *
     * <p>The operation is considered colocated because {@link Type#BROADCAST_DISTRIBUTED broadcast}
     * distribution satisfies any other distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAffinityAndBroadcast(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
                .and(input(isInstanceOf(setOp.colocated)
                        // TODO https://issues.apache.org/jira/browse/IGNITE-18211
                        // .and(hasDistribution(IgniteDistributions.affinity(0, null, "hash")))
                        .and(input(0, isTableScan("affinity_tbl1")))
                        .and(input(1, isInstanceOf(IgniteTrimExchange.class)
                                .and(input(isTableScan("broadcast_tbl1")))
                        ))
                ))
        );
    }

    /**
     * Tests SET operations on tables with different affinity distribution.
     *
     * <p>Different affinity distributions cannot be colocated.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18211")
    public void testSetOpNonColocatedAffinity(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl3 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("affinity_tbl1")))
                        .and(input(1, isTableScan("affinity_tbl2")))
                        .and(input(1, isTableScan("affinity_tbl3")))
                ))
        );

        sql = "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl4 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("affinity_tbl1")))
                        .and(input(1, isTableScan("affinity_tbl4")))
                ))
        );
    }

    /**
     * Tests two SET operations (nested and outer) on two tables with the same affinity distribution.
     *
     * <p>Nested operation is considered colocated because the tables are compared against the corresponding collocation columns.
     * Outer operation considered colocated because the result of nested operation must have the distribution of one of the participating
     * tables.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAffinityNested(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM affinity_tbl2 " + setOp(setOp) + "("
                + "   SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "   SELECT * FROM affinity_tbl2"
                + ")";

        assertPlan(sql, publicSchema, isInstanceOf(IgniteExchange.class)
                        .and(input(isInstanceOf(setOp.colocated)
                                .and(input(0, isTableScan("affinity_tbl2")))
                                .and(input(1, isInstanceOf(setOp.colocated)
                                        .and(input(0, isTableScan("affinity_tbl1")))
                                        .and(input(1, isTableScan("affinity_tbl2")))
                                ))
                        )),
                "MinusMergeRule", "IntersectMergeRule"
        );
    }

    /**
     * Tests SET operations on two tables with broadcast and random distribution.
     *
     * <p>The operation is considered colocated because {@link Type#BROADCAST_DISTRIBUTED broadcast}
     * distribution satisfies any other distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpBroadcastAndRandom(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM broadcast_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                .and(input(0, hasChildThat(isTableScan("random_tbl1"))))
                .and(input(1, isTableScan("broadcast_tbl1")))
        );
    }

    /**
     * Tests two SET operations (nested and outer) on two tables with random distribution.
     *
     * <p>Nested operation cannot be colocated because {@link Type#RANDOM_DISTRIBUTED random}
     * distribution cannot be colocated with other random distribution.
     * Outer operation considered colocated because the result of nested operation must have
     * the distribution of one of the participating tables.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpRandomNested(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl2 "
                + setOp(setOp) + "("
                + "   SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "   SELECT * FROM random_tbl2"
                + ")";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                        .and(input(0, hasChildThat(isTableScan("random_tbl2"))))
                        .and(input(1, isInstanceOf(setOp.reduce)
                                .and(hasChildThat(isInstanceOf(setOp.map)
                                        .and(input(0, isTableScan("random_tbl1")))
                                        .and(input(1, isTableScan("random_tbl2")))
                                ))
                        )),
                "IntersectMergeRule"
        );
    }

    /**
     * Tests two SET operations (nested and outer) on three tables with two random (nested) and one broadcast (outer) distribution.
     *
     * <p>Nested operation cannot be colocated because {@link Type#RANDOM_DISTRIBUTED random}
     * distribution cannot be colocated with other random distribution.
     * Outer operation considered colocated because because {@link Type#BROADCAST_DISTRIBUTED broadcast}
     * distribution satisfies any other distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpBroadcastAndRandomNested(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM broadcast_tbl1 "
                + setOp(setOp)
                + "("
                + "   SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "   SELECT * FROM random_tbl2"
                + ")";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.colocated)
                        .and(input(0, isTableScan("broadcast_tbl1")))
                        .and(input(1, isInstanceOf(setOp.reduce)
                                .and(hasChildThat(isInstanceOf(setOp.map)
                                        .and(input(0, isTableScan("random_tbl1")))
                                        .and(input(1, isTableScan("random_tbl2")))
                                ))
                        )),
                "IntersectMergeRule"
        );
    }

    /**
     * Tests multiple SET operations on multiple tables with affinity and random distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpMerge(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM random_tbl2 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl1 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce)
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("random_tbl1")))
                        .and(input(1, isTableScan("random_tbl2")))
                        .and(input(2, isTableScan("affinity_tbl1")))
                        .and(input(3, isTableScan("affinity_tbl2")))
                ))
        );
    }

    /**
     * Tests multiple SET operations (with ALL flag enabled) on multiple tables with affinity and random distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAllMerge(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM random_tbl2 "
                + setOpAll(setOp)
                + "SELECT * FROM affinity_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM affinity_tbl2 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(IgniteSetOp::all)
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("random_tbl1")))
                        .and(input(1, isTableScan("random_tbl2")))
                        .and(input(2, isTableScan("affinity_tbl1")))
                        .and(input(3, isTableScan("affinity_tbl2")))
                ))
        );
    }

    /**
     * Tests two SET operations (with ALL flag enabled for the first one) on tables with affinity and random distribution.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource
    public void testSetOpAllWithExceptMerge(SetOp setOp) throws Exception {
        String sql = "SELECT * FROM random_tbl1 "
                + setOpAll(setOp)
                + "SELECT * FROM random_tbl2 "
                + setOp(setOp)
                + "SELECT * FROM affinity_tbl1 ";

        assertPlan(sql, publicSchema, isInstanceOf(setOp.reduce).and(n -> !n.all())
                .and(hasChildThat(isInstanceOf(setOp.map)
                        .and(input(0, isTableScan("random_tbl1")))
                        .and(input(1, isTableScan("random_tbl2")))
                        .and(input(2, isTableScan("affinity_tbl1")))
                ))
        );
    }

    private String setOp(SetOp setOp) {
        return setOp.name() + ' ';
    }

    private String setOpAll(SetOp setOp) {
        return setOp.name() + " ALL ";
    }

    enum SetOp {
        EXCEPT(
                IgniteColocatedMinus.class,
                IgniteMapMinus.class,
                IgniteReduceMinus.class
        ),

        INTERSECT(
                IgniteColocatedIntersect.class,
                IgniteMapIntersect.class,
                IgniteReduceIntersect.class
        );

        public final Class<? extends IgniteColocatedSetOp> colocated;

        public final Class<? extends IgniteMapSetOp> map;

        public final Class<? extends IgniteReduceSetOp> reduce;

        SetOp(
                Class<? extends IgniteColocatedSetOp> colocated,
                Class<? extends IgniteMapSetOp> map,
                Class<? extends IgniteReduceSetOp> reduce) {
            this.colocated = colocated;
            this.map = map;
            this.reduce = reduce;
        }
    }
}
