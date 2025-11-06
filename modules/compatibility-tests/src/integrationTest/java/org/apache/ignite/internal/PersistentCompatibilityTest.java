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

import static org.apache.ignite.internal.jobs.DeploymentUtils.runJob;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.io.IOException;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.compute.CheckpointJob;
import org.apache.ignite.internal.jobs.DeploymentUtils;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Legend: v1 is original version, v2 is current version (version from main branch).
 *
 * <p>
 * It is supposed that data is written in v1, then the storage is flushed and the node is stopped, its binary is replaced with v2, the node
 * is started.
 *
 * <p>
 * The tests have to ensure that aipersist-based tables work correctly:
 * <ul>
 * <li> All data written in v1 can be read in v2. </li>
 * <li> Including overwritten versions. </li>
 * <li> Deleted data cannot be read. </li>
 * <li> Updates that are rolled back cannot be read. </li>
 * </ul>
 *
 * <p>
 * Variations:
 * <ul>
 * <li> When there were no unmerged delta files in v1 (that is, after the writes are finished on
 * v1, we wait for all delta files to be merged and removed). </li>
 * <li>When there were some unmerged delta-files. </li>
 * <li> When unmerged delta-files remain from v1 and we start creating new delta files immediately after starting on v2, and THEN we
 * start verifications listed above (while the delta files are still there). </li>
 * </ul>
 */
@ParameterizedClass
@MethodSource("baseVersions")
@MicronautTest(rebuildContext = true)
// In older versions ThreadAssertingStorageEngine doesn't implement wrapper interface, so it's not possible to cast it to
// PersistentPageMemoryStorageEngine
@WithSystemProperty(key = IgniteSystemProperties.THREAD_ASSERTIONS_ENABLED, value = "false")
public class PersistentCompatibilityTest extends CompatibilityTestBase {
    /** Delta files are not compacted before updating the cluster. */
    private static final String TABLE_WITH_DELTA_FILES = "TEST_WITH_DELTA_FILES";

    /** Delta files are compacted before updating the cluster. */
    private static final String TABLE_WITHOUT_DELTA_FILES = "TEST_WITHOUT_DELTA_FILES";

    /** Delta files are not compacted before updating the cluster, and new ones created after. */
    private static final String TABLE_WITH_NEW_DELTA_FILES = "TEST_WITH_NEW_DELTA_FILES";

    private static final int UNCHANGED_ROW_ID = 1;
    private static final int DELETED_ROW_ID = 2;
    private static final int UPDATED_ROW_ID = 3;
    private static final int ROLLED_BACK_ROW_ID = 4;

    private static final String UNCHANGED_ROW_VALUE = "unchanged_value";
    private static final String ORIGINAL_ROW_VALUE = "original_value";
    private static final String UPDATED_ROW_VALUE = "updated_value";

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        try {
            DeploymentUtils.deployJobs();

            createAndPopulateTable(baseIgnite, TABLE_WITHOUT_DELTA_FILES);
            createAndPopulateTable(baseIgnite, TABLE_WITH_DELTA_FILES);
            createAndPopulateTable(baseIgnite, TABLE_WITH_NEW_DELTA_FILES);

            // Newly allocated pages are written straight to the page files.
            // We need to do an initial checkpoint so subsequent modifications of the rows go to delta files.
            doCheckpointWithCompaction();

            prepareTable(baseIgnite, TABLE_WITHOUT_DELTA_FILES);

            // Checkpoint for TABLE_WITHOUT_DELTA_FILES and compact all delta files.
            doCheckpointWithCompaction();

            prepareTable(baseIgnite, TABLE_WITH_DELTA_FILES);
            prepareTable(baseIgnite, TABLE_WITH_NEW_DELTA_FILES);

            // Checkpoint for TABLE_WITH_DELTA_FILES and TABLE_WITH_NEW_DELTA_FILES, cancels compaction to leave delta files.
            doCheckpointWithoutCompaction();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void createAndPopulateTable(Ignite baseIgnite, String tableName) {
        sql(baseIgnite, "CREATE TABLE " + tableName + " (id INT PRIMARY KEY, name VARCHAR)");
        insertRow(baseIgnite, tableName, UNCHANGED_ROW_ID, UNCHANGED_ROW_VALUE);
        insertRow(baseIgnite, tableName, DELETED_ROW_ID, "deleted_value");
        insertRow(baseIgnite, tableName, UPDATED_ROW_ID, ORIGINAL_ROW_VALUE);
        insertRow(baseIgnite, tableName, ROLLED_BACK_ROW_ID, ORIGINAL_ROW_VALUE);
    }

    private static void prepareTable(Ignite baseIgnite, String tableName) {
        sql(baseIgnite, "DELETE FROM " + tableName + " WHERE id = " + DELETED_ROW_ID);

        updateRow(baseIgnite, null, tableName, UPDATED_ROW_ID, UPDATED_ROW_VALUE);

        Transaction tx = baseIgnite.transactions().begin();
        updateRow(baseIgnite, tx, tableName, ROLLED_BACK_ROW_ID, UPDATED_ROW_VALUE);
        tx.rollback();
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_WITH_DELTA_FILES, TABLE_WITHOUT_DELTA_FILES})
    void testNewVersion(String tableName) {
        checkRows(tableName);
    }

    @Test
    void testNewVersionWithNewDeltaFiles() throws IOException {
        String newRowValue = "new_row";

        insertRow(node(0), TABLE_WITH_NEW_DELTA_FILES, 5, newRowValue);

        updateRow(node(0), null, TABLE_WITH_NEW_DELTA_FILES, UNCHANGED_ROW_ID, UPDATED_ROW_VALUE);

        doCheckpointWithoutCompaction();

        List<List<Object>> rows = sql("select * from " + TABLE_WITH_NEW_DELTA_FILES + " order by id");

        assertThat(rows.size(), is(4));
        assertThat(rows.get(0).get(1), is(UPDATED_ROW_VALUE));
        assertThat(rows.get(1).get(1), is(UPDATED_ROW_VALUE));
        assertThat(rows.get(2).get(1), is(ORIGINAL_ROW_VALUE));
        assertThat(rows.get(3).get(1), is(newRowValue));
    }

    private void doCheckpointWithCompaction() throws IOException {
        doCheckpoint(false);
    }

    private void doCheckpointWithoutCompaction() throws IOException {
        doCheckpoint(true);
    }

    private void doCheckpoint(boolean cancelCompaction) {
        runJob(cluster, CheckpointJob.class, cancelCompaction);
    }

    private static void insertRow(Ignite baseIgnite, String tableName, int id, String name) {
        sql(baseIgnite, "INSERT INTO " + tableName + " (id, name) VALUES (?, ?)", id, name);
    }

    private static void updateRow(Ignite baseIgnite, @Nullable Transaction tx, String tableName, int id, String value) {
        sql(baseIgnite, tx, "UPDATE " + tableName + " SET name = ? WHERE id = ?", value, id);
    }

    private void checkRows(String tableName) {
        List<List<Object>> rows = sql("select * from " + tableName + " order by id");

        assertThat(rows.size(), is(3));
        assertThat(rows.get(0).get(1), is(UNCHANGED_ROW_VALUE));
        assertThat(rows.get(1).get(1), is(UPDATED_ROW_VALUE));
        assertThat(rows.get(2).get(1), is(ORIGINAL_ROW_VALUE));
    }
}
