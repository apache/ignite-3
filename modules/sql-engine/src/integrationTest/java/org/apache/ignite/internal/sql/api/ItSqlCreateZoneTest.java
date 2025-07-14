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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.List;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ItSqlCreateZoneTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_MANE = "test_zone";
    private static final String NOT_EXISTED_PROFILE_NAME = "not-existed-profile";
    private static final String EXTRA_PROFILE_NAME = "extra-profile";
    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + EXTRA_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnSameNode() {
        assertDoesNotThrow(() -> createZoneQuery(0, "default"));
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNode() {
        cluster.startNode(1, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);
        assertDoesNotThrow(() -> createZoneQuery(0, EXTRA_PROFILE_NAME));
    }

    @Test
    void testCreateZoneFailedWithoutCorrectStorageProfileInCluster() {
        assertThrowsWithCode(
                SqlException.class,
                STMT_VALIDATION_ERR,
                () -> createZoneQuery(0, NOT_EXISTED_PROFILE_NAME),
                "Some storage profiles don't exist [missedProfileNames=[" + NOT_EXISTED_PROFILE_NAME + "]]."
        );
    }

    private  List<List<Object>> createZoneQuery(int nodeIdx, String storageProfile) {
        return executeSql(nodeIdx, format("CREATE ZONE IF NOT EXISTS {} STORAGE PROFILES ['{}']", ZONE_MANE, storageProfile));
    }
}
