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

package org.apache.ignite.internal.cli.commands.zone.datanodes;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;

import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Base test class for recalculate data nodes commands. */
public abstract class ItRecalculateDataNodesTest extends CliIntegrationTest {
    private static final String ZONE1 = "test_zone_1";
    private static final String ZONE2 = "test_zone_2";

    @BeforeAll
    public void createZones() {
        sql(String.format("CREATE ZONE \"%s\" storage profiles['%s']", ZONE1, DEFAULT_AIPERSIST_PROFILE_NAME));
        sql(String.format("CREATE ZONE \"%s\" storage profiles['%s']", ZONE2, DEFAULT_AIPERSIST_PROFILE_NAME));
    }

    @Test
    public void testRecalculateAllZones() {
        execute(CLUSTER_URL_OPTION, NODE_URL);

        assertErrOutputIsEmpty();
        assertOutputContains("Done");
    }

    @Test
    public void testRecalculateSpecifiedZones() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                "--zone-names", ZONE1 + "," + ZONE2);

        assertErrOutputIsEmpty();
        assertOutputContains("Done");
    }

    @Test
    public void testRecalculateSingleZone() {
        execute(CLUSTER_URL_OPTION, NODE_URL,
                "--zone-names", ZONE1);

        assertErrOutputIsEmpty();
        assertOutputContains("Done");
    }

    @Test
    public void testRecalculateNonExistentZone() {
        String unknownZone = "unknown_zone";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                "--zone-names", unknownZone);

        assertErrOutputContains("Distribution zones were not found");
        assertErrOutputContains(unknownZone);
        assertOutputIsEmpty();
    }

    @Test
    public void testRecalculateMixedZones() {
        String unknownZone = "unknown_zone";

        execute(CLUSTER_URL_OPTION, NODE_URL,
                "--zone-names", ZONE1 + "," + unknownZone);

        assertErrOutputContains("Distribution zones were not found");
        assertErrOutputContains(unknownZone);
        assertOutputIsEmpty();
    }
}
