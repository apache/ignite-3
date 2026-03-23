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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ItSqlAlterZoneTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_MANE = "test_zone";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testAlterZoneFailsIfFilterIsInvalid() {
        executeSql(0, format("CREATE ZONE IF NOT EXISTS {} STORAGE PROFILES ['default']", ZONE_MANE));

        String filter = "$[?(@.region == \"non-existing\")]";

        assertThrowsWithCode(
                SqlException.class,
                STMT_VALIDATION_ERR,
                () -> alterZoneQueryWithFilter(0, filter),
                format("Node filter does not match any node in the cluster [filter='{}'].", filter)
        );
    }

    private void alterZoneQueryWithFilter(int nodeIdx, String filter) {
        executeSql(nodeIdx, format("ALTER ZONE {} SET (NODES FILTER '{}') ", ZONE_MANE, filter));
    }
}
