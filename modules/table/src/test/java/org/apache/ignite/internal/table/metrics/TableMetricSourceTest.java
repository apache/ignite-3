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

package org.apache.ignite.internal.table.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.table.QualifiedName;
import org.junit.jupiter.api.Test;

/**
 * Tests metric source name and table metric names.
 * If you want to change the name, or add a new metric, please don't forget to update the corresponding documentation.
 */
public class TableMetricSourceTest {
    private static final String TABLE_NAME = "test_table";

    @Test
    void testMetricSourceName() {
        QualifiedName qualifiedTableName = QualifiedName.fromSimple(TABLE_NAME);

        var metricSource = new TableMetricSource(qualifiedTableName);

        assertThat(TableMetricSource.SOURCE_NAME, is("tables"));
        assertThat(metricSource.name(), is("tables." + qualifiedTableName.toCanonicalForm()));
    }

    @Test
    void testMetricNames() {
        var metricSource = new TableMetricSource(QualifiedName.fromSimple(TABLE_NAME));

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "RwReads",
                "RoReads",
                "Writes");

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }
}
