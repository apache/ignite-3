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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.CompatibilityTestCommon.TABLE_NAME_TEST;
import static org.apache.ignite.internal.CompatibilityTestCommon.createDefaultTables;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Compatibility tests for metastorage raft log. */
@ParameterizedClass
@MethodSource("baseVersions")
@MicronautTest(rebuildContext = true)
// Old version node starts with disabled colocation.
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
public class MetastorageRaftCompatibilityTest extends CompatibilityTestBase {

    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        return false;
    }

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        createDefaultTables(baseIgnite);;

        Path nodeWorkDir = workDir.resolve("cluster").resolve(cluster.getRunnerNodes().get(0).nodeName());

        cluster.stop();

        // To force metastorage recovery from the raft log.
        deleteMetastorageDbDir(nodeWorkDir);
    }

    @AfterEach
    void tearDown()  {
        cluster.stop();
    }

    @Test
    void testReapplication() {
        cluster.startEmbedded(1, false);

        checkMetastorage(cluster.node(0));
    }

    @Test
    void testStreamToFollower() throws InterruptedException {
        cluster.startEmbedded(2, false);

        checkMetastorage(cluster.node(0));

        MetaStorageManager newNodeMetastorage = unwrapIgniteImpl(cluster.node(1)).metaStorageManager();
        MetaStorageManager oldNodeMetastorage = unwrapIgniteImpl(cluster.node(0)).metaStorageManager();

        // Assert that new node got all log entries from old one.
        assertTrue(waitForCondition(() -> oldNodeMetastorage.appliedRevision() == newNodeMetastorage.appliedRevision(), 10_000));
    }

    private static void checkMetastorage(Ignite ignite) {
        // Won't work if metastorage is not restored correctly.
        sql(ignite, "SELECT * FROM " + TABLE_NAME_TEST);
    }

    private static void deleteMetastorageDbDir(Path nodeWorkDir) {
        try {
            IgniteUtils.deleteIfExistsThrowable(new ComponentWorkingDir(nodeWorkDir.resolve("metastorage")).dbPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
