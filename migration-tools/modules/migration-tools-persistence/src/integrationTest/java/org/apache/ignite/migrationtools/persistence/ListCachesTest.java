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

package org.apache.ignite.migrationtools.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;

/** ListCachesTest. */
@DisabledIfSystemProperty(
        named = "tests.containers.support",
        matches = "false",
        disabledReason = "Lack of support in TeamCity for testcontainers")
@ExtendWith(FullSampleCluster.class)
public class ListCachesTest {

    @ExtendWith(BasePersistentTestContext.class)
    private List<MigrationKernalContext> nodeContexts;

    @BeforeEach
    void startContexts() throws IgniteCheckedException {
        for (MigrationKernalContext ctx : nodeContexts) {
            ctx.start();
        }
        // Stop is done automatically.
    }

    @Test
    void checkCachesAreInTheSampleCluster() throws IgniteCheckedException {
        var foundCaches = Ignite2PersistentCacheTools.persistentCaches(nodeContexts);
        assertThat(foundCaches).hasSize(8);
        // Value extract from the seeddata-container.log
    }

}
