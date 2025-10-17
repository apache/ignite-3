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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Partition raft log compatibility tests for one node. */
@ParameterizedClass
@MethodSource("baseVersions")
public class ItPartitionRaftLogOneNodeCompatibilityTest extends CompatibilityTestBase {
    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        return false;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        sql(baseIgnite, String.format("CREATE TABLE %s(ID INT PRIMARY KEY, VAL VARCHAR)", TABLE_NAME));

        String insertDml = String.format("INSERT INTO %s (ID, VAL) VALUES (?, ?)", TABLE_NAME);

        baseIgnite.transactions().runInTransaction(tx -> {
            for (int i = 0; i < 10; i++) {
                sql(baseIgnite, tx, insertDml, i, "str_" + i);
            }
        });
    }

    /** Tests a simple node restart scenario with reapplying a partition raft log. */
    @Test
    void testSimpleRestart() {
        cluster.stop();

        cleanTableStoragesDir();

        cluster.startEmbedded(nodesCount());

        assertThat(sql(String.format("SELECT * FROM %s", TABLE_NAME)), hasSize(10));
    }

    private void cleanTableStoragesDir() {
        Path dbDir = workDir.resolve(cluster.clusterName()).resolve(cluster.nodeName(0)).resolve("partitions").resolve("db");

        assertThat(dbDir.toString(), isDirectoryExistsAndNotEmpty(dbDir), is(true));

        assertThat(dbDir.toString(), IgniteUtils.deleteIfExists(dbDir), is(true));
    }

    private static boolean isDirectoryExistsAndNotEmpty(Path path) {
        if (!Files.exists(path) || !Files.isDirectory(path)) {
            return false;
        }

        try (Stream<Path> list = Files.list(path)) {
            return list.findAny().isPresent();
        } catch (IOException e) {
            return false;
        }
    }
}
