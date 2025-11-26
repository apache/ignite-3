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

import static org.apache.ignite.internal.sql.engine.trait.IgniteDistributions.single;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.TableBuilder;
import org.apache.ignite.internal.sql.engine.rel.IgniteAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceAggregateBase;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * This is a base class for aggregate-related tests aimed to verify integration of aggregate nodes
 * into the planner.
 *
 * <p>Tests derived from this class can be separated into two groups.
 *
 * <p>The first one verifies that expected algorithm is chosen by optimiser in the particular test
 * case. This is more like verification of cost function implementation. The rule of thumbs for this
 * group is that no optimisation rules should be disabled (there may be exceptions though).
 *
 * <p>The second group verifies that every test query can be prepared with only single algorithm
 * available. This is more like verification of traits propagation. The rule of thumb for this
 * group is that for every algorithm, rest of them should be disabled.
 *
 * <p>The base class defines a number of test cases represented by {@link TestCase} enum. Every test
 * case must be verified by the derived class, otherwise exception will be thrown.
 *
 * <p>New test must be added as new element of the {@link TestCase} enumeration only.Please check
 * if it fits to any of the groups mentioned above. If it doesn't, then, probably, it's better to
 * find another place.
 */
public abstract class AbstractAggregatePlannerTest extends AbstractPlannerTest {
    private static final Predicate<AggregateCall> NON_NULL_PREDICATE = Objects::nonNull;

    /**
     * Enumeration of test cases.
     *
     * <p>Every derived class must call {@link #assertPlan(TestCase, Predicate, String...)} for every case from this enum,
     * otherwise afterAll validation will fail.
     */
    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    enum TestCase {
        /**
         * Query: SELECT SUM(val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_1("SELECT SUM(val0) FROM test", schema(single())),
        /**
         * Query: SELECT SUM(val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_1A("SELECT SUM(val0) FROM test", schema(hash())),
        /**
         * Query: SELECT SUM(val0) FROM test.
         *
         * <p>Distribution: identity(0)
         */
        CASE_1B("SELECT SUM(val0) FROM test", schema(identity())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_2_1("SELECT SUM(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_2_1A("SELECT SUM(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(0)
         */
        CASE_2_1B("SELECT SUM(DISTINCT val0) FROM test", schema(identity())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(1)
         */
        CASE_2_1C("SELECT SUM(DISTINCT val0) FROM test", schema(hash(1))),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(1)
         */
        CASE_2_1D("SELECT SUM(DISTINCT val0) FROM test", schema(identity(1))),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_2_2("SELECT COUNT(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_2_2A("SELECT COUNT(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(0)
         */
        CASE_2_2B("SELECT COUNT(DISTINCT val0) FROM test", schema(identity())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(1)
         */
        CASE_2_2C("SELECT COUNT(DISTINCT val0) FROM test", schema(hash(1))),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(1)
         */
        CASE_2_2D("SELECT COUNT(DISTINCT val0) FROM test", schema(identity(1))),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_3_1("SELECT MIN(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_3_1A("SELECT MIN(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(0)
         */
        CASE_3_1B("SELECT MIN(DISTINCT val0) FROM test", schema(identity())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(1)
         */
        CASE_3_1C("SELECT MIN(DISTINCT val0) FROM test", schema(hash(1))),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(1)
         */
        CASE_3_1D("SELECT MIN(DISTINCT val0) FROM test", schema(identity(1))),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: single
         */
        CASE_3_2("SELECT MAX(DISTINCT val0) FROM test", schema(single())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(0)
         */
        CASE_3_2A("SELECT MAX(DISTINCT val0) FROM test", schema(hash())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(0)
         */
        CASE_3_2B("SELECT MAX(DISTINCT val0) FROM test", schema(identity())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: hash(1)
         */
        CASE_3_2C("SELECT MAX(DISTINCT val0) FROM test", schema(hash(1))),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test.
         *
         * <p>Distribution: identity(1)
         */
        CASE_3_2D("SELECT MAX(DISTINCT val0) FROM test", schema(identity(1))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_5("SELECT SUM(val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_5A("SELECT SUM(val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(0)
         */
        CASE_5B("SELECT SUM(val0) FROM test GROUP BY grp0", schema(identity())),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(3)
         */
        CASE_5C("SELECT SUM(val0) FROM test GROUP BY grp0", schema(hash(3))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(3)
         */
        CASE_5D("SELECT SUM(val0) FROM test GROUP BY grp0", schema(identity(3))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution single
         */
        CASE_6("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(single())),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_6A("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(hash())),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution identity(0)
         */
        CASE_6B("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(identity())),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution hash(3, 4)
         */
        CASE_6C("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(hash(3, 4))),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_1("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_1A("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(0)
         */
        CASE_7_1B("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(identity())),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(3)
         */
        CASE_7_1C("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(hash(3))),
        /**
         * Query: SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(3)
         */
        CASE_7_1D("SELECT COUNT(DISTINCT val0) FROM test GROUP BY grp0", schema(identity(3))),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_2("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_2A("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(0)
         */
        CASE_7_2B("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(identity())),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(3)
         */
        CASE_7_2C("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(hash(3))),
        /**
         * Query: SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(3)
         */
        CASE_7_2D("SELECT grp0, COUNT(DISTINCT val0) as v1 FROM test GROUP BY grp0", schema(identity(3))),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution single
         */
        CASE_7_3("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(single())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_7_3A("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(hash())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(0)
         */
        CASE_7_3B("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(identity())),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution hash(3)
         */
        CASE_7_3C("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(hash(3))),
        /**
         * Query: SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0.
         *
         * <p>Distribution identity(3)
         */
        CASE_7_3D("SELECT SUM(DISTINCT val0) FROM test GROUP BY grp0", schema(identity(3))),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution single
         */
        CASE_8_1("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(single())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_8_1A("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(hash())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution identity(0)
         */
        CASE_8_1B("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(identity())),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution hash(2)
         */
        CASE_8_1C("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(hash(2))),
        /**
         * Query: SELECT MIN(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution identity(2)
         */
        CASE_8_1D("SELECT MIN(DISTINCT val0) FROM test GROUP BY val1", schema(identity(2))),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution single
         */
        CASE_8_2("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(single())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_8_2A("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(hash())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution identity(0)
         */
        CASE_8_2B("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(identity())),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution hash(2)
         */
        CASE_8_2C("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(hash(2))),
        /**
         * Query: SELECT MAX(DISTINCT val0) FROM test GROUP BY val1.
         *
         * <p>Distribution identity(2)
         */
        CASE_8_2D("SELECT MAX(DISTINCT val0) FROM test GROUP BY val1", schema(identity(2))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution single
         */
        CASE_9("SELECT SUM(val0) FROM test GROUP BY grp0", schema(single(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution hash(0)
         */
        CASE_9A("SELECT SUM(val0) FROM test GROUP BY grp0", schema(hash(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution identity(0)
         */
        CASE_9B("SELECT SUM(val0) FROM test GROUP BY grp0", schema(identity(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution hash(3)
         */
        CASE_9C("SELECT SUM(val0) FROM test GROUP BY grp0", schema(hash(3), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution identity(3)
         */
        CASE_9D("SELECT SUM(val0) FROM test GROUP BY grp0", schema(identity(3), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0, grp1.
         *
         * <p>Distribution single
         */
        CASE_10("SELECT SUM(val0) FROM test GROUP BY grp0, grp1", schema(single(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0, grp1.
         *
         * <p>Distribution hash(0)
         */
        CASE_10A("SELECT SUM(val0) FROM test GROUP BY grp0, grp1", schema(hash(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0, grp1.
         *
         * <p>Distribution identity(0)
         */
        CASE_10B("SELECT SUM(val0) FROM test GROUP BY grp0, grp1", schema(identity(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp0, grp1.
         *
         * <p>Distribution hash(3, 4)
         */
        CASE_10C("SELECT SUM(val0) FROM test GROUP BY grp0, grp1", schema(hash(3, 4), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution single
         */
        CASE_11("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(single(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution hash(0)
         */
        CASE_11A("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(hash(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution identity(0)
         */
        CASE_11B("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(identity(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT SUM(val0) FROM test GROUP BY grp1, grp0.
         *
         * <p>Distribution hash(4, 3)
         */
        CASE_11C("SELECT SUM(val0) FROM test GROUP BY grp1, grp0", schema(hash(4, 3), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution single
         */
        CASE_12("SELECT DISTINCT val0, val1 FROM test", schema(single(), addSortIndex("val0"))),
        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution hash(0)
         */
        CASE_12A("SELECT DISTINCT val0, val1 FROM test", schema(hash(), addSortIndex("val0"))),
        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution identity(0)
         */
        CASE_12B("SELECT DISTINCT val0, val1 FROM test", schema(identity(), addSortIndex("val0"))),
        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution hash(1)
         */
        CASE_12C("SELECT DISTINCT val0, val1 FROM test", schema(hash(1), addSortIndex("val0"))),

        /**
         * Query: SELECT DISTINCT val0, val1 FROM test.
         *
         * <p>Index on val0
         *
         * <p>Distribution identity(1)
         */
        CASE_12D("SELECT DISTINCT val0, val1 FROM test", schema(identity(1), addSortIndex("val0"))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution single
         */
        CASE_13("SELECT DISTINCT grp0, grp1 FROM test", schema(single(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution hash(0)
         */
        CASE_13A("SELECT DISTINCT grp0, grp1 FROM test", schema(hash(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution identity(0)
         */
        CASE_13B("SELECT DISTINCT grp0, grp1 FROM test", schema(identity(), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution hash(3)
         */
        CASE_13C("SELECT DISTINCT grp0, grp1 FROM test", schema(hash(3), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT DISTINCT grp0, grp1 FROM test.
         *
         * <p>Index on (grp0, grp1)
         *
         * <p>Distribution identity(3)
         */
        CASE_13D("SELECT DISTINCT grp0, grp1 FROM test", schema(identity(3), addSortIndex("grp0", "grp1"))),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = (SELECT SUM(val1) FROM test).
         *
         * <p>Distribution single
         */
        CASE_14("SELECT val0 FROM test WHERE VAL1 = (SELECT SUM(val1) FROM test)", schema(single())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = (SELECT SUM(val1) FROM test).
         *
         * <p>Distribution hash(0)
         */
        CASE_14A("SELECT val0 FROM test WHERE VAL1 = (SELECT SUM(val1) FROM test)", schema(hash())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = (SELECT SUM(val1) FROM test).
         *
         * <p>Distribution identity(0)
         */
        CASE_14B("SELECT val0 FROM test WHERE VAL1 = (SELECT SUM(val1) FROM test)", schema(identity())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test).
         *
         * <p>Distribution single
         */
        CASE_15("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(single())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test).
         *
         * <p>Distribution hash(0)
         */
        CASE_15A("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(hash())),
        /**
         * Query: SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test).
         *
         * <p>Distribution identity(0)
         */
        CASE_15B("SELECT val0 FROM test WHERE VAL1 = ANY(SELECT DISTINCT val1 FROM test)", schema(identity())),
        /**
         * Query: SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test).
         *
         * <p>Index on val0 DESC
         *
         * <p>Distribution single
         */
        CASE_16("SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)", schema(single(), indexByVal0Desc())),
        /**
         * Query: SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test).
         *
         * <p>Index on val0 DESC
         *
         * <p>Distribution hash(0)
         */
        CASE_16A("SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)", schema(hash(), indexByVal0Desc())),
        /**
         * Query: SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test).
         *
         * <p>Index on val0 DESC
         *
         * <p>Distribution identity(0)
         */
        CASE_16B("SELECT ID FROM test WHERE VAL0 IN (SELECT VAL0 FROM test)", schema(identity(), indexByVal0Desc())),
        /**
         * Query: SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 2) FROM test.
         *
         * <p>Distribution single
         */
        CASE_17("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 2) FROM test", schema(single())),
        /**
         * Query: SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 2) FROM test.
         *
         * <p>Distribution hash(0)
         */
        CASE_17A("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 2) FROM test", schema(hash())),
        /**
         * Query: SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 2) FROM test.
         *
         * <p>Distribution identity(0)
         */
        CASE_17B("SELECT (SELECT test.val0 FROM test t ORDER BY 1 LIMIT 2) FROM test", schema(identity())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1.
         *
         * <p>Distribution single
         */
        CASE_18_1("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_18_1A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1.
         *
         * <p>Distribution identity(0)
         */
        CASE_18_1B("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1", schema(identity())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0.
         *
         * <p>Distribution single
         */
        CASE_18_2("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0.
         *
         * <p>Distribution hash(0)
         */
        CASE_18_2A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0.
         *
         * <p>Distribution identity(0)
         */
        CASE_18_2B("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1, val0", schema(identity())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1.
         *
         * <p>Distribution single
         */
        CASE_18_3("SELECT val1, val0, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_18_3A("SELECT val1, val0, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1.
         *
         * <p>Distribution identity(0)
         */
        CASE_18_3B("SELECT val1, val0, COUNT(*) cnt FROM test GROUP BY val1, val0 ORDER BY val0, val1", schema(identity())),
        /**
         * SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0.
         *
         * <p>Distribution single
         */
        CASE_19_1("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(single())),
        /**
         * SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0.
         *
         * <p>Distribution hash(0)
         */
        CASE_19_1A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(hash())),
        /**
         * SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0.
         *
         * <p>Distribution identity(0)
         */
        CASE_19_1B("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0", schema(identity())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1.
         *
         * <p>Distribution single
         */
        CASE_19_2("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1.
         *
         * <p>Distribution hash(0)
         */
        CASE_19_2A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1.
         *
         * <p>Distribution identity(0)
         */
        CASE_19_2B("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1", schema(identity())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt.
         *
         * <p>Distribution single
         */
        CASE_20("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt", schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt.
         *
         * <p>Distribution hash(0)
         */
        CASE_20A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt", schema(hash())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt.
         *
         * <p>Distribution identity(0)
         */
        CASE_20B("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val0, val1, cnt", schema(identity())),
        /**
         * Query: SELECT /*+ EXPAND_DISTINCT_AGG *&#47; SUM(DISTINCT val0), SUM(DISTINCT val1) FROM test GROUP BY grp0.
         *
         * <p>Index on val0
         *
         * <p>Distribution single
         */
        CASE_21("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), SUM(DISTINCT val1) FROM test GROUP BY grp0",
                schema(single(), addSortIndex("grp0", "val0"), addSortIndex("grp0", "val1"))),
        /**
         * Query: SELECT /*+ EXPAND_DISTINCT_AGG *&#47; SUM(DISTINCT val0), SUM(DISTINCT val1) FROM test GROUP BY grp0.
         *
         * <p>Index on val0
         *
         * <p>Distribution hash(0)
         */
        CASE_21A("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), SUM(DISTINCT val1) FROM test GROUP BY grp0",
                schema(hash(), addSortIndex("grp0", "val0"), addSortIndex("grp0", "val1"))),
        /**
         * Query: SELECT /*+ EXPAND_DISTINCT_AGG *&#47; SUM(DISTINCT val0), SUM(DISTINCT val1) FROM test GROUP BY grp0.
         *
         * <p>Index on val0
         *
         * <p>Distribution identity(0)
         */
        CASE_21B("SELECT /*+ EXPAND_DISTINCT_AGG */ SUM(DISTINCT val0), SUM(DISTINCT val1) FROM test GROUP BY grp0",
                        schema(identity(), addSortIndex("grp0", "val0"), addSortIndex("grp0", "val1"))),
        /**
         * Query: SELECT val0, COUNT(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution hash(0)
         */
        CASE_22("SELECT val0, COUNT(val1) FROM test GROUP BY val0", schema(hash())),
        /**
         * Query: SELECT val0, COUNT(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution identity(0)
         */
        CASE_22A("SELECT val0, COUNT(val1) FROM test GROUP BY val0", schema(identity())),
        /**
         * Query: SELECT val0, COUNT(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution hash(1)
         */
        CASE_22B("SELECT val0, COUNT(val1) FROM test GROUP BY val0", schema(hash(1))),
        /**
         * Query: SELECT val0, COUNT(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution identity(1)
         */
        CASE_22C("SELECT val0, COUNT(val1) FROM test GROUP BY val0", schema(identity(1))),
        /**
         * Query: SELECT val0, AVG(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution hash(0)
         */
        CASE_23("SELECT val0, AVG(val1) FROM test GROUP BY val0", schema(hash())),
        /**
         * Query: SELECT val0, AVG(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution identity(0)
         */
        CASE_23A("SELECT val0, AVG(val1) FROM test GROUP BY val0", schema(identity())),
        /**
         * Query: SELECT val0, AVG(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution hash(1)
         */
        CASE_23B("SELECT val0, AVG(val1) FROM test GROUP BY val0", schema(hash(1))),
        /**
         * Query: SELECT val0, AVG(val1) FROM test GROUP BY val0.
         *
         * <p>Distribution identity(1)
         */
        CASE_23C("SELECT val0, AVG(val1) FROM test GROUP BY val0", schema(identity(1))),

        /**
         * Query: SELECT COUNT(val0), COUNT(DISTINCT(val1) from test.
         *
         * <p>Distribution single()
         */
        CASE_24_1("SELECT COUNT(val0), COUNT(DISTINCT(val1)) from test", schema(single())),

        /**
         * Query: SELECT COUNT(val0), COUNT(DISTINCT(val1) from test.
         *
         * <p>Distribution hash(0)
         */
        CASE_24_1A("SELECT COUNT(val0), COUNT(DISTINCT(val1)) from test", schema(hash(0))),

        /**
         * Query: SELECT COUNT(val0), COUNT(DISTINCT(val1) from test.
         *
         * <p>Distribution hash(1)
         */
        CASE_24_1B("SELECT COUNT(val0), COUNT(DISTINCT(val1)) from test", schema(hash(1))),

        /**
         * Query: SELECT COUNT(val0), COUNT(DISTINCT(val1) from test.
         *
         * <p>Distribution hash(2)
         */
        CASE_24_1C("SELECT COUNT(val0), COUNT(DISTINCT(val1)) from test", schema(hash(2))),

        /**
         * Query: SELECT COUNT(val0), COUNT(DISTINCT(val1) from test.
         *
         * <p>Distribution identity(1)
         */
        CASE_24_1D("SELECT COUNT(val0), COUNT(DISTINCT(val1)) from test", schema(identity(1))),

        /**
         * Query: SELECT COUNT(val0), COUNT(DISTINCT(val1) from test.
         *
         * <p>Distribution identity(2)
         */
        CASE_24_1E("SELECT COUNT(val0), COUNT(DISTINCT(val1)) from test", schema(identity(2))),

        /**
         * Query: SELECT val0, COUNT(val1) FROM test GROUP BY val0 ORDER BY val0 DESC.
         *
         * <p>Distribution single()
         */
        CASE_25("SELECT val0, COUNT(val1) FROM test GROUP BY val0 ORDER BY val0 DESC", schema(single())),
        /**
         * Query: SELECT val0, COUNT(val1) FROM test GROUP BY val0 ORDER BY val0 DESC.
         *
         * <p>Distribution hash(0)
         */
        CASE_25A("SELECT val0, COUNT(val1) FROM test GROUP BY val0 ORDER BY val0 DESC", schema(hash(0))),

        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1 DESC.
         *
         * <p>Distribution single
         */
        CASE_26("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1 DESC",
                schema(single())),
        /**
         * Query: SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1 DESC.
         *
         * <p>Distribution hash(0)
         */
        CASE_26A("SELECT val0, val1, COUNT(*) cnt FROM test GROUP BY val0, val1 ORDER BY val1 DESC",
                schema(hash(0))),

        /**
         * Query: SELECT val0 FROM test WHERE val0 = (SELECT val1 FROM test).
         *
         * <p>Distribution single()
         */
        CASE_27("SELECT val0 FROM test WHERE val0 = (SELECT val1 FROM test)", schema(single())),

        /**
         * Query: INSERT INTO test (id, val0) VALUES (1, (SELECT val1 FROM test)).
         *
         * <p>Distribution single()
         */
        CASE_27A("INSERT INTO test (id, val0) VALUES (1, (SELECT val1 FROM test))", schema(single())),

        /**
         * Query: UPDATE test set val0 = (SELECT val1 FROM test).
         *
         * <p>Distribution single()
         */
        CASE_27B("UPDATE test set val0 = (SELECT val1 FROM test)", schema(single())),

        /**
         * Query: MERGE INTO test as t0 USING test as t1 ON t0.id = t1.id WHEN MATCHED THEN UPDATE SET val1 = (SELECT val0 FROM test).
         *
         * <p>Distribution single()
         */
        CASE_27C("MERGE INTO test as t0 USING test as t1 ON t0.id = t1.id "
                + "WHEN MATCHED THEN UPDATE SET val1 = (SELECT val0 FROM test)", schema(single())),

        /**
         * Query: SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0)).
         *
         * <p>Distribution single()
         */
        CASE_28_1A("SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0))", schema(single())),

        /**
         * Query: SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0), (grp1)).
         *
         * <p>Distribution single()
         */
        CASE_28_1B("SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0), (grp1))", schema(single())),

        /**
         * Query: SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0)).
         *
         * <p>Distribution hash(1)
         */
        CASE_28_2A("SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0))", schema(hash(1))),

        /**
         * Query: SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0), (grp1)).
         *
         * <p>Distribution hash(1)
         */
        CASE_28_2B("SELECT GROUPING(grp0) from test GROUP BY GROUPING SETS ((grp0), (grp1))", schema(hash(1))),
        ;

        final String query;
        final IgniteSchema schema;

        TestCase(String query, IgniteSchema schema) {
            this.query = query;
            this.schema = schema;
        }

        @Override
        public String toString() {
            return this.name() + ": query=" + query;
        }
    }

    private static EnumSet<TestCase> missedCases;

    @BeforeAll
    static void initMissedCases() {
        missedCases = EnumSet.allOf(TestCase.class);
    }

    @AfterAll
    static void ensureAllCasesAreCovered() {
        assertThat("Some cases were not covered by test", missedCases, Matchers.empty());
    }

    /**
     * Verifies given test case with provided predicate.
     *
     * <p>That is, applies predicate to the result of the query optimization with regards to the
     * provided collection of rules which have to be disabled during optimization.
     *
     * @param testCase A test case to verify.
     * @param predicate A predicate to validate resulting plan. If predicate returns false,
     *     the AssertionFailedError will be thrown.
     * @param rulesToDisable A collection of rules to disable.
     * @param <T> An expected type of the root node of resulting plan.
     */
    protected <T extends RelNode> void assertPlan(
            TestCase testCase,
            Predicate<T> predicate,
            String... rulesToDisable
    ) throws Exception {
        if (!missedCases.remove(testCase)) {
            fail("Testcase was as disabled: " + testCase);
        }

        assertPlan(testCase.query, Collections.singleton(testCase.schema), predicate, List.of(), rulesToDisable);
    }

    protected void assumeRun(TestCase testCase) {
        boolean removed = missedCases.remove(testCase);

        assertTrue(removed, "Unapplicable/Ignored test case was unexpectedly run.");
    }

    @SafeVarargs
    private static IgniteSchema schema(IgniteDistribution distribution,
            UnaryOperator<TableBuilder>... indices) {

        Function<TableBuilder, TableBuilder> testTable = defaultTestTable(distribution);

        for (UnaryOperator<TableBuilder> index : indices) {
            testTable = testTable.andThen(index);
        }

        return createSchemaFrom(testTable);
    }

    private static UnaryOperator<TableBuilder> defaultTestTable(IgniteDistribution distribution) {
        return t -> t.name("TEST")
                .addColumn("ID", NativeTypes.INT32)
                .addColumn("VAL0", NativeTypes.INT32)
                .addColumn("VAL1", NativeTypes.INT32)
                .addColumn("GRP0", NativeTypes.INT32)
                .addColumn("GRP1", NativeTypes.INT32)
                .size(DEFAULT_TBL_SIZE)
                .distribution(distribution);
    }

    private static IgniteDistribution hash() {
        return TestBuilders.affinity(0, nextTableId(), DEFAULT_ZONE_ID);
    }

    private static IgniteDistribution hash(int... keys) {
        return TestBuilders.affinity(IntList.of(keys), nextTableId(), DEFAULT_ZONE_ID);
    }

    private static IgniteDistribution identity() {
        return IgniteDistributions.identity(0);
    }

    private static IgniteDistribution identity(int key) {
        return IgniteDistributions.identity(key);
    }

    private static UnaryOperator<TableBuilder> indexByVal0Desc() {
        return tableBuilder -> tableBuilder
                .sortedIndex()
                .name("idx_val0")
                .addColumn("VAL0", Collation.DESC_NULLS_FIRST)
                .end();
    }

    <T extends RelNode> Predicate<T> hasAggregate() {
        Predicate<T> mapNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(NON_NULL_PREDICATE));
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class)
                .and(n -> n.getAggregateCalls().stream().anyMatch(NON_NULL_PREDICATE));

        return mapNode.or(reduceNode);
    }

    <T extends RelNode> Predicate<T> hasSingleValueAggregate() {
        return (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream()
                        .anyMatch(agg -> agg.getAggregation() instanceof SqlSingleValueAggFunction));
    }

    <T extends RelNode> Predicate<T> hasDistinctAggregate() {
        Predicate<T> mapNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class)
                .and(n -> n.getAggCallList().stream().anyMatch(NON_NULL_PREDICATE.and(AggregateCall::isDistinct)));
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class)
                .and(n -> n.getAggregateCalls().stream().anyMatch(NON_NULL_PREDICATE.and(AggregateCall::isDistinct)));

        return mapNode.or(reduceNode);
    }

    <T extends RelNode> Predicate<T> hasGroups() {
        Predicate<T> aggregateNode = (Predicate<T>) isInstanceOf(IgniteAggregate.class).and(n -> !n.getGroupSets().isEmpty());
        Predicate<T> reduceNode = (Predicate<T>) isInstanceOf(IgniteReduceAggregateBase.class).and(n -> !n.getGroupSets().isEmpty());

        return aggregateNode.or(reduceNode);
    }

    Predicate<List<AggregateCall>> hasAggregates(Predicate<AggregateCall>... calls) {
        return (list) -> {
            if (list.size() != calls.length) {
                return false;
            }

            for (int i = 0; i < calls.length; i++) {
                AggregateCall actual = list.get(i);
                Predicate<AggregateCall> expected = calls[i];
                if (!expected.test(actual)) {
                    return false;
                }
            }

            return true;
        };
    }
}
