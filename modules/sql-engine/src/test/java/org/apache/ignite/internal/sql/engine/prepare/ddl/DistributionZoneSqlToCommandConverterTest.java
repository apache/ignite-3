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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.SetDefaultZoneEntry;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.partitiondistribution.DistributionAlgorithm;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the conversion of a sql zone definition to a command.
 */
@SuppressWarnings("ThrowableNotThrown")
public class DistributionZoneSqlToCommandConverterTest extends AbstractDdlSqlToCommandConverterTest {

    private static final List<ZoneOptionEnum> NUMERIC_OPTIONS = List.of(
            ZoneOptionEnum.PARTITIONS,
            ZoneOptionEnum.REPLICAS,
            ZoneOptionEnum.QUORUM_SIZE,
            ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP,
            ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN
    );

    private static final List<ZoneOptionEnum> STRING_OPTIONS = List.of(
            ZoneOptionEnum.DISTRIBUTION_ALGORITHM,
            ZoneOptionEnum.DATA_NODES_FILTER,
            ZoneOptionEnum.CONSISTENCY_MODE
    );

    private static final String AIPERSIST_STORAGE_PROFILE = "segmented_aipersist";

    private static final String ROCKSDB_STORAGE_PROFILE = "lru_rocks";

    private static final List<String> NODE_DEFAULT_STORAGE_PROFILES = List.of(
            DEFAULT_STORAGE_PROFILE,
            AIPERSIST_STORAGE_PROFILE,
            ROCKSDB_STORAGE_PROFILE
    );

    private LogicalTopologyService logicalTopologyService;

    @BeforeEach
    public void setUp() {
        // Default mock
        logicalTopologyService = mock(LogicalTopologyService.class);

        LogicalTopologySnapshot defaultLogicalTopologySnapshot = new LogicalTopologySnapshot(
                0,
                IntStream.range(0, 2)
                        .mapToObj(nodeIdx -> createLocalNode(nodeIdx, NODE_DEFAULT_STORAGE_PROFILES))
                        .collect(Collectors.toList())
        );

        when(logicalTopologyService.localLogicalTopology()).thenReturn(defaultLogicalTopologySnapshot);

        when(logicalTopologyService.logicalTopologyOnLeader()).thenReturn(completedFuture(defaultLogicalTopologySnapshot));

        Supplier<TableStatsStalenessConfiguration> statStalenessProperties = () -> new TableStatsStalenessConfiguration(
                DEFAULT_STALE_ROWS_FRACTION, DEFAULT_MIN_STALE_ROWS_COUNT);

        converter = new DdlSqlToCommandConverter(
                new ClusterWideStorageProfileValidator(logicalTopologyService),
                filter -> completedFuture(null), statStalenessProperties
        );

        assertThat(ZoneOptionEnum.values().length, is(NUMERIC_OPTIONS.size() + STRING_OPTIONS.size()));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZone(boolean withPresent) throws SqlParseException {
        CatalogCommand cmd = convert(withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'"
                : "CREATE ZONE test STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']");

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.name(), equalTo("TEST"));

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.STRONG_CONSISTENCY));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithConsistencyModeStrongConsistency(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH storage_profiles='" + DEFAULT_STORAGE_PROFILE + "',"
                        + " CONSISTENCY_MODE='" + ConsistencyMode.STRONG_CONSISTENCY.name() + "'"
                : "CREATE ZONE test (CONSISTENCY MODE '" + ConsistencyMode.STRONG_CONSISTENCY.name() + "') "
                        + "STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "'] ";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.STRONG_CONSISTENCY));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithConsistencyModeHighAvailability(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                        + " CONSISTENCY_MODE='" + ConsistencyMode.HIGH_AVAILABILITY + "'"
                : "CREATE ZONE test (CONSISTENCY MODE '" + ConsistencyMode.HIGH_AVAILABILITY + "') "
                        + "STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "'] ";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.consistencyMode(), equalTo(ConsistencyMode.HIGH_AVAILABILITY));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithConsistencyModeInvalid(boolean withPresent) {
        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "',"
                        + " CONSISTENCY_MODE='MY_CUSTOM_MODE'"
                : "CREATE ZONE test (CONSISTENCY MODE 'MY_CUSTOM_MODE') "
                        + "STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "'] ";

        assertThrows(
                SqlException.class,
                () -> convert(sql),
                "Failed to parse consistency mode: MY_CUSTOM_MODE. Valid values are: [STRONG_CONSISTENCY, HIGH_AVAILABILITY]"
        );
    }

    @Test
    public void testMixedOptions() {
        String passed = "CREATE ZONE test with partitions=2, replicas=3, storage_profiles='p' S";
        assertThrowsWithPos("CREATE ZONE test with partitions=2, replicas=3, storage_profiles='p' STORAGE PROFILES ['profile']",
                "STORAGE", passed.length());

        passed = "CREATE ZONE test with (";
        assertThrowsWithPos("CREATE ZONE test with (partitions 2, replicas 3, storage_profiles='p') STORAGE PROFILES ['profile']",
                "(", passed.length());
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithOptions(boolean withPresent) throws SqlParseException {
        // Check non-conflicting options.
        {
            String sql = withPresent
                    ? "CREATE ZONE test with "
                            + "partitions=2, "
                            + "replicas=5, "
                            + "quorum_size=2, " // non-default value
                            + "distribution_algorithm='rendezvous', "
                            + "data_nodes_filter='$[?(@.region == \"US\")]', "
                            + "storage_profiles='" + ROCKSDB_STORAGE_PROFILE + " , " + AIPERSIST_STORAGE_PROFILE + " ' "
                    : "CREATE ZONE test "
                            + "(partitions 2, "
                            + "replicas 5, "
                            + "quorum size 2, " // non-default value
                            + "distribution algorithm 'rendezvous', "
                            + "nodes filter '$[?(@.region == \"US\")]') "
                            + "storage profiles ['" + ROCKSDB_STORAGE_PROFILE + "' , '" + AIPERSIST_STORAGE_PROFILE + " '] ";

            CatalogCommand cmd = convert(sql);
            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

            assertThat(desc.partitions(), equalTo(2));
            assertThat(desc.replicas(), equalTo(5));
            assertThat(desc.quorumSize(), equalTo(2));
            // TODO https://issues.apache.org/jira/browse/IGNITE-22162
            // assertThat(desc.distributionAlgorithm(), equalTo("rendezvous"));
            assertThat(desc.filter(), equalTo("$[?(@.region == \"US\")]"));

            List<CatalogStorageProfileDescriptor> storageProfiles = desc.storageProfiles().profiles();
            assertThat(storageProfiles, hasSize(2));
            assertThat(storageProfiles.get(0).storageProfile(), equalTo(ROCKSDB_STORAGE_PROFILE));
            assertThat(storageProfiles.get(1).storageProfile(), equalTo(AIPERSIST_STORAGE_PROFILE));
        }

        // Check remaining options.
        {
            String sql = withPresent
                    ? "CREATE ZONE test with "
                            + "data_nodes_auto_adjust_scale_up=100, "
                            + "data_nodes_auto_adjust_scale_down=200, "
                            + "storage_profiles='" + ROCKSDB_STORAGE_PROFILE + "'"
                    : "CREATE ZONE test "
                            + "(auto scale up 100, "
                            + "auto scale down 200) "
                            + "storage profiles ['" + ROCKSDB_STORAGE_PROFILE + "']";

            CatalogCommand cmd = convert(sql);
            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

            assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(100));
            assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(200));
        }

        // Check option validation.
        if (withPresent) {
            expectOptionValidationError("CREATE ZONE test with partitions=-1, storage_profiles='p'", "PARTITION");
            expectOptionValidationError("CREATE ZONE test with replicas=-1, storage_profiles='p'", "REPLICAS");
            assertThrowsWithPos("CREATE ZONE test with replicas=FALL, storage_profiles='p'", "FALL", 32);
            emptyProfilesValidationError("CREATE ZONE test with storage_profiles='' ");
            emptyProfilesValidationError("CREATE ZONE test with storage_profiles=' ' ");
        } else {
            assertThrowsWithPos("CREATE ZONE test (partitions -1)", "-", 30);
            assertThrowsWithPos("CREATE ZONE test (AUTO SCALE FALL)", "FALL", 30);
            assertThrowsWithPos("CREATE ZONE test (AUTO SCALE UP FALL)", "FALL", 33);
            assertThrowsWithPos("CREATE ZONE test (AUTO SCALE DOWN FALL)", "FALL", 35);
            assertThrowsWithPos("CREATE ZONE test (replicas -1)", "-", 28);
            assertThrowsWithPos("CREATE ZONE test (replicas FALL)", "FALL", 28);
            assertThrowsWithPos("CREATE ZONE test (replicas 1, partitions -1)", "-", 42);
            assertThrowsWithPos("CREATE ZONE test storage_profiles ['']", "storage_profiles", 18);
            assertThrowsParseException("CREATE ZONE test (distribution algorithm '')", "Validation Error: Empty character literal "
                    + "is not allowed in this context.");
            assertThrowsParseException("CREATE ZONE test storage profiles ['']", "Validation Error: Empty character literal "
                    + "is not allowed in this context.");
            assertThrowsParseException("CREATE ZONE test storage profiles [' ']", "Validation Error: Empty character literal "
                    + "is not allowed in this context.");
        }
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithReplicasAll(boolean withPresent) throws SqlParseException {
        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "', REPLICAS=ALL"
                : "CREATE ZONE test (REPLICAS ALL) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.replicas(), equalTo(DistributionAlgorithm.ALL_REPLICAS));
    }

    @Test
    public void testCreateZoneWithScaleOff() throws SqlParseException {
        String sql = "CREATE ZONE test (AUTO SCALE UP OFF) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(INFINITE_TIMER_VALUE));

        sql = "CREATE ZONE test (AUTO SCALE DOWN OFF) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        cmd = convert(sql);

        desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(INFINITE_TIMER_VALUE));
    }

    @Test
    public void testCreateZoneWithAllScaleOff() throws SqlParseException {
        String sql = "CREATE ZONE test (AUTO SCALE OFF) STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(INFINITE_TIMER_VALUE));
        assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(INFINITE_TIMER_VALUE));
    }

    @Test
    public void testAlterZoneWithScaleOff() throws SqlParseException {
        CatalogCommand cmd = convert("ALTER ZONE test SET (AUTO SCALE UP OFF)");

        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        mockCatalogZone("TEST");

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(INFINITE_TIMER_VALUE));

        cmd = convert("ALTER ZONE test SET (AUTO SCALE DOWN OFF)");

        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(INFINITE_TIMER_VALUE));
    }

    @Test
    public void testAlterZoneWithAllScaleOff() throws SqlParseException {
        CatalogCommand cmd = convert("ALTER ZONE test SET (AUTO SCALE OFF)");

        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        mockCatalogZone("TEST");

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(INFINITE_TIMER_VALUE));
        assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(INFINITE_TIMER_VALUE));
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testSingleNonExistedStorageProfile(boolean withPresent) {
        String nonExistedStorageProfileName = "not_existed_profile";

        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + nonExistedStorageProfileName + "'"
                : "CREATE ZONE test STORAGE PROFILES ['" + nonExistedStorageProfileName + "']";

        expectStatementValidationError(
                sql,
                "Some storage profiles don't exist [missedProfileNames=[" + nonExistedStorageProfileName + "]]."
        );
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testSeveralNonExistedStorageProfiles(boolean withPresent) {
        String nonExistedStorageProfileName1 = "not_existed_profile_1";
        String nonExistedStorageProfileName2 = "not_existed_profile_2";

        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + nonExistedStorageProfileName1 + ", " + nonExistedStorageProfileName2 + "'"
                : "CREATE ZONE test STORAGE PROFILES ['" + nonExistedStorageProfileName1 + "', '" + nonExistedStorageProfileName2 + "']";

        expectStatementValidationError(
                sql,
                "Some storage profiles don't exist [missedProfileNames=["
                        + nonExistedStorageProfileName1 + ", "
                        + nonExistedStorageProfileName2 + "]]."
        );
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testNonExistedStorageProfilesAmongExistedOnes(boolean withPresent) {
        String nonExistedStorageProfileName = "not_existed_profile";

        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='"
                        + AIPERSIST_STORAGE_PROFILE + ", "
                        + nonExistedStorageProfileName + ", "
                        + ROCKSDB_STORAGE_PROFILE + "'"
                : "CREATE ZONE test STORAGE PROFILES ['"
                        + AIPERSIST_STORAGE_PROFILE + "', '"
                        + nonExistedStorageProfileName + "', '"
                        + ROCKSDB_STORAGE_PROFILE + "']";

        expectStatementValidationError(
                sql,
                "Some storage profiles don't exist [missedProfileNames=[" + nonExistedStorageProfileName + "]]."
        );
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testExistedStorageProfileOnDisjointProfileSetsInLogicalTopologySnapshot(boolean withPresent) throws SqlParseException {
        when(logicalTopologyService.localLogicalTopology()).thenReturn(new LogicalTopologySnapshot(
                0,
                List.of(
                        createLocalNode(0, List.of(AIPERSIST_STORAGE_PROFILE)),
                        createLocalNode(1, List.of(ROCKSDB_STORAGE_PROFILE)),
                        createLocalNode(2, List.of(DEFAULT_STORAGE_PROFILE))
                )
        ));

        String sql = withPresent
                ? "CREATE ZONE test WITH STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'"
                : "CREATE ZONE test STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        CatalogCommand cmd = convert(sql);

        List<CatalogStorageProfileDescriptor> storageProfiles = invokeAndGetFirstEntry(cmd, NewZoneEntry.class)
                .descriptor()
                .storageProfiles()
                .profiles();
        assertThat(storageProfiles, hasSize(1));
        assertThat(storageProfiles.get(0).storageProfile(), equalTo(DEFAULT_STORAGE_PROFILE));
    }

    private static List<Arguments> defaultQuorum() {
        return List.of(
                Arguments.of(1, 1),
                Arguments.of(2, 2),
                Arguments.of(5, 3),
                Arguments.of(10, 3)
        );
    }

    @ParameterizedTest(name = "replicas = {0}, expectedQuorum = {1}")
    @MethodSource("defaultQuorum")
    public void testCreateZoneWithQuorumDefault(int replicas, int expectedQuorum) throws SqlParseException {
        String sql = "CREATE ZONE test WITH REPLICAS=" + replicas + ", STORAGE_PROFILES='" + DEFAULT_STORAGE_PROFILE + "'";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.quorumSize(), equalTo(expectedQuorum));
    }

    private static List<Arguments> correctQuorumSize() {
        return List.of(
                Arguments.of(1, 1),
                Arguments.of(2, 2),
                Arguments.of(5, 2),
                Arguments.of(10, 5)
        );
    }

    @ParameterizedTest(name = "(replicas = {0}, quorum size = {1})")
    @MethodSource("correctQuorumSize")
    public void testCreateZoneWithQuorum(int replicas, int quorum) throws SqlParseException {
        String sql = "CREATE ZONE test (REPLICAS " + replicas + ", QUORUM SIZE " + quorum
                + ") STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        CatalogCommand cmd = convert(sql);

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, NewZoneEntry.class).descriptor();

        assertThat(desc.quorumSize(), equalTo(quorum));
    }

    private static List<Arguments> invalidQuorumSize() {
        return List.of(
                Arguments.of(1, 2),
                Arguments.of(2, 3),
                Arguments.of(5, 4),
                Arguments.of(10, 1)
        );
    }

    @ParameterizedTest(name = "(replicas {0}, quorum size {1})")
    @MethodSource("invalidQuorumSize")
    public void testCreateZoneWithInvalidQuorum(int replicas, int quorum) {
        String sql = "CREATE ZONE test (REPLICAS " + replicas + ", QUORUM SIZE " + quorum
                + ") STORAGE PROFILES ['" + DEFAULT_STORAGE_PROFILE + "']";

        // We can't properly validate quorum size in the parser, so the exception is thrown from the catalog command validation instead.
        assertThrows(
                CatalogValidationException.class,
                () -> convert(sql),
                "Specified quorum size doesn't fit into the specified replicas count"
        );
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithoutStorageProfileOptionShouldThrowError(boolean withPresent) {
        assertThrowsWithPos("CREATE ZONE test", "<EOF>", 16);

        if (withPresent) {
            emptyProfilesValidationError("CREATE ZONE test with replicas=1");
        } else {
            assertThrowsWithPos("CREATE ZONE test (replicas 1)", "<EOF>", 29);
        }
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void testCreateZoneWithDuplicateOptions(boolean withPresent) {
        String sql = withPresent
                ? "CREATE ZONE test with partitions=2, replicas=0, PARTITIONS=1, STORAGE_PROFILES='profile'"
                : "CREATE ZONE test (partitions 2, replicas 0, PARTITIONS 1) STORAGE PROFILES ['profile']";

        expectDuplicateOptionError(sql, "PARTITIONS");
    }

    @Test
    public void testRenameZoneCommand() throws SqlParseException {
        CatalogCommand cmd = convert("ALTER ZONE test RENAME TO test2");

        assertThat(cmd, instanceOf(RenameZoneCommand.class));

        when(catalog.zone("TEST")).thenReturn(mock(CatalogZoneDescriptor.class));

        AlterZoneEntry entry = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class);

        assertThat(entry.descriptor().name(), equalTo("TEST2"));
        assertThat(((RenameZoneCommand) cmd).ifExists(), is(false));
    }

    @Test
    public void testRenameZoneIfExistCommand() throws SqlParseException {
        CatalogCommand cmd = convert("ALTER ZONE IF EXISTS test RENAME TO test2");

        assertThat(cmd, instanceOf(RenameZoneCommand.class));

        RenameZoneCommand zoneCmd = (RenameZoneCommand) cmd;

        when(catalog.zone("TEST")).thenReturn(mock(CatalogZoneDescriptor.class));

        AlterZoneEntry entry = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class);

        assertThat(entry.descriptor().name(), equalTo("TEST2"));
        assertThat(zoneCmd.ifExists(), is(true));
    }

    @ParameterizedTest(name = "obsolete = {0}")
    @ValueSource(booleans = {true, false})
    public void testAlterZoneCommand(boolean obsolete) throws SqlParseException {
        CatalogCommand cmd = convert(obsolete
                ? "ALTER ZONE test SET replicas=5, quorum_size=3"
                : "ALTER ZONE test SET (replicas 5, quorum size 3)"
        );

        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        mockCatalogZone("TEST");

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.name(), equalTo("TEST"));
        assertThat(desc.replicas(), is(5));
        assertThat(desc.quorumSize(), is(3));
        assertThat(((AlterZoneCommand) cmd).ifExists(), is(false));
    }

    @ParameterizedTest(name = "obsolete = {0}")
    @ValueSource(booleans = {true, false})
    public void testAlterZoneIfExistsCommand(boolean obsolete) throws SqlParseException {
        CatalogCommand cmd = convert(obsolete ? "ALTER ZONE IF EXISTS test SET replicas=3" : "ALTER ZONE IF EXISTS test SET (replicas 3)");

        assertThat(cmd, instanceOf(AlterZoneCommand.class));
        assertThat(((AlterZoneCommand) cmd).ifExists(), is(true));
    }

    @ParameterizedTest(name = "obsolete = {0}")
    @ValueSource(booleans = {true, false})
    public void testAlterZoneSetCommand(boolean obsolete) throws SqlParseException {
        // Check non-conflicting options.
        {
            String sql = obsolete
                    ? "ALTER ZONE test SET "
                            + "replicas=5, "
                            + "quorum_size=3, "
                            + "data_nodes_filter='$[?(@.region == \"US\")]'"
                    : "ALTER ZONE test SET "
                            + "(replicas 5, "
                            + "quorum size 3, "
                            + "nodes filter '$[?(@.region == \"US\")]')";

            CatalogCommand cmd = convert(sql);

            assertThat(cmd, instanceOf(AlterZoneCommand.class));

            mockCatalogZone("TEST");

            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

            assertThat(desc.name(), equalTo("TEST"));

            assertThat(desc.replicas(), equalTo(5));
            assertThat(desc.quorumSize(), equalTo(3));
            assertThat(desc.filter(), equalTo("$[?(@.region == \"US\")]"));
        }

        // Check remaining options.
        {
            String sql = obsolete
                    ? "ALTER ZONE test SET "
                            + "data_nodes_auto_adjust_scale_up=100, "
                            + "data_nodes_auto_adjust_scale_down=200"
                    : "ALTER ZONE test SET "
                            + "(auto scale up 100, "
                            + "auto scale down 200)";

            CatalogCommand cmd = convert(sql);

            assertThat(cmd, instanceOf(AlterZoneCommand.class));

            mockCatalogZone("TEST");

            CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

            assertThat(desc.name(), equalTo("TEST"));

            assertThat(desc.dataNodesAutoAdjustScaleUp(), equalTo(100));
            assertThat(desc.dataNodesAutoAdjustScaleDown(), equalTo(200));
        }
    }

    @ParameterizedTest(name = "obsolete = {0}")
    @ValueSource(booleans = {true, false})
    public void testAlterZoneReplicasAll(boolean obsolete) throws SqlParseException {
        CatalogCommand cmd = convert(obsolete ? "ALTER ZONE test SET replicas=ALL" : "ALTER ZONE test SET (replicas ALL)");

        assertThat(cmd, instanceOf(AlterZoneCommand.class));

        mockCatalogZone("TEST");

        CatalogZoneDescriptor desc = invokeAndGetFirstEntry(cmd, AlterZoneEntry.class).descriptor();

        assertThat(desc.replicas(), is(DistributionAlgorithm.ALL_REPLICAS));
    }

    @Test
    public void testAlterZoneSetDefault() throws SqlParseException {
        CatalogCommand cmd = convert("ALTER ZONE test SET DEFAULT");

        assertThat(cmd, instanceOf(AlterZoneSetDefaultCommand.class));

        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
        when(catalog.zone("TEST")).thenReturn(zoneMock);

        SetDefaultZoneEntry entry = invokeAndGetFirstEntry(cmd, SetDefaultZoneEntry.class);

        AlterZoneSetDefaultCommand zoneCmd = (AlterZoneSetDefaultCommand) cmd;
        assertThat(entry.zoneId(), is(zoneMock.id()));
        assertThat(zoneCmd.ifExists(), is(false));
    }

    @Test
    public void testAlterZoneSetDefaultIfExists() throws SqlParseException {
        CatalogCommand cmd = convert("ALTER ZONE IF EXISTS test SET DEFAULT");

        assertThat(cmd, instanceOf(AlterZoneSetDefaultCommand.class));

        assertThat(((AlterZoneSetDefaultCommand) cmd).ifExists(), is(true));
    }

    @ParameterizedTest(name = "obsolete = {0}, option = {1}")
    @MethodSource("numericOptions")
    public void testAlterZoneCommandWithInvalidOptions(boolean obsolete, ZoneOptionEnum option) {
        String sql = obsolete
                ? "ALTER ZONE test SET {}=-100"
                : "ALTER ZONE test SET ({} -100)";

        if (obsolete) {
            expectOptionValidationError(format(sql, option.name()), option.name());
        } else {
            String sqlName = option.sqlName;
            String prefix = "ALTER ZONE test SET (";
            assertThrowsWithPos(format(sql, sqlName, "-100"), "-", prefix.length() + sqlName.length() + 1 /* start pos*/
                    + 1 /* first symbol after bracket*/);
        }
    }

    @Test
    public void testDropZone() throws SqlParseException {
        CatalogCommand cmd = convert("DROP ZONE test");

        assertThat(cmd, instanceOf(DropZoneCommand.class));

        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
        when(catalog.zone("TEST")).thenReturn(zoneMock);

        DropZoneEntry entry = invokeAndGetFirstEntry(cmd, DropZoneEntry.class);

        assertThat(entry.zoneId(), is(zoneMock.id()));
    }

    @ParameterizedTest(name = "with syntax = {0}, option = {1}")
    @MethodSource("numericOptions")
    public void createZoneWithInvalidNumericOptionValue(boolean withPresent, ZoneOptionEnum option) {
        String sql = withPresent ? "create zone test_zone with {}={}, storage_profiles='p'" : "create zone test_zone ({} {})";

        if (withPresent) {
            expectInvalidOptionType(format(sql, option, "'bar'"), option.name());
        } else {
            String sqlName = option.sqlName;
            String prefix = "create zone test_zone (";
            int errorPos = prefix.length() + sqlName.length() + 1 /* start pos*/ + 1 /* first symbol after bracket*/;

            assertThrowsWithPos(format(sql, sqlName, "'bar'"), "\\'bar\\'", errorPos);
            assertThrowsWithPos(format(sql, sqlName, "-1"), "-", errorPos);
        }
    }

    @ParameterizedTest(name = "with syntax = {0}")
    @ValueSource(booleans = {true, false})
    public void createZoneWithUnexpectedOption(boolean withPresent) {
        String sql = withPresent ? "create zone test_zone with ABC=1, storage_profiles='p'" : "create zone test_zone (ABC 1)";

        if (withPresent) {
            expectUnexpectedOption(sql, "ABC");
        } else {
            assertThrowsWithPos(sql, "ABC", 24);
        }
    }

    @ParameterizedTest(name = "with syntax = {0}, option = {1}")
    @MethodSource("stringOptions")
    public void createZoneWithInvalidStringOptionValue(boolean withPresent, ZoneOptionEnum option) {
        if (withPresent) {
            String sql = format("create zone test_zone with {}={}, storage_profiles='p'", option.name(), "1");
            expectInvalidOptionType(sql, option.name());
        } else {
            String sql = "create zone test_zone ({} {})";

            String sqlName = option.sqlName;
            String prefix = "create zone test_zone (";
            assertThrowsWithPos(format(sql, sqlName, "1"), "1", prefix.length() + sqlName.length() + 1 /* start pos*/
                    + 1 /* first symbol after bracket*/);
        }
    }

    @Test
    public void createZoneWithInvalidStorageProfiles() {
        String profiles = "STORAGE PROFILES [";
        String sql = "create zone test_zone {} {}]";

        String prefix = "create zone test_zone ";

        assertThrowsWithPos(format(sql, profiles, "1"), "1", prefix.length() + profiles.length() + 1 /* start pos*/
                + 1 /* first symbol after bracket*/);
    }

    @ParameterizedTest
    @MethodSource("pureNumericOptions")
    public void alterZoneWithInvalidNumericOptionValue(ZoneOptionEnum optionParam) {
        String sql = format("alter zone test_zone set {}={}", optionParam.name(), "'bar'");
        expectInvalidOptionType(sql, optionParam.name());
    }

    @Test
    public void alterZoneWithUnexpectedOption() {
        expectUnexpectedOption("alter zone test_zone set ABC=1", "ABC");
    }

    private void assertThrowsWithPos(String query, String encountered, int pos) {
        assertThrowsParseException(query, format("Encountered \"{}\" at line 1, column {}.", encountered, pos));
    }

    private void assertThrowsParseException(String query, String message) {
        assertThrows(SqlParseException.class, () -> convert(query), message);
    }

    private static Stream<ZoneOptionEnum> pureNumericOptions() {
        return NUMERIC_OPTIONS.stream();
    }

    private static Stream<Arguments> numericOptions() {
        return Stream.of(true, false).flatMap(t1 -> NUMERIC_OPTIONS.stream()
                .map(t2 -> Arguments.of(t1, t2))
        );
    }

    private static Stream<Arguments> stringOptions() {
        return Stream.of(true, false).flatMap(t1 -> STRING_OPTIONS.stream()
                .map(t2 -> Arguments.of(t1, t2))
        );
    }

    private void expectOptionValidationError(String sql, String invalidOption) {
        expectStatementValidationError(sql, "Zone option validation failed [option=" + invalidOption);
    }

    private void emptyProfilesValidationError(String sql) {
        expectStatementValidationError(sql, "STORAGE PROFILES can not be empty");
    }

    private void expectInvalidOptionType(String sql, String invalidOption) {
        expectStatementValidationError(sql, "Invalid zone option type [option=" + invalidOption);
    }

    private void expectUnexpectedOption(String sql, String invalidOption) {
        expectStatementValidationError(sql, "Unexpected zone option [option=" + invalidOption);
    }

    private void expectDuplicateOptionError(String sql, String option) {
        expectStatementValidationError(sql, "Duplicate zone option has been specified [option=" + option);
    }

    private void expectStatementValidationError(String sql, String errorMessageFragment) {
        assertThrowsWithCode(
                SqlException.class,
                STMT_VALIDATION_ERR,
                () -> convert(sql),
                errorMessageFragment
        );
    }

    private static LogicalNode createLocalNode(int nodeIdx, List<String> storageProfiles) {
        return new LogicalNode(
                new ClusterNodeImpl(
                        UUID.randomUUID(),
                        "node" + nodeIdx,
                        new NetworkAddress("127.0.0.1", 3344 + nodeIdx)
                ),
                Map.of(),
                Map.of(),
                storageProfiles
        );
    }

    private void mockCatalogZone(String zoneName) {
        CatalogZoneDescriptor zoneMock = mock(CatalogZoneDescriptor.class);
        when(zoneMock.name()).thenReturn(zoneName);
        when(zoneMock.filter()).thenReturn("");

        when(catalog.zone("TEST")).thenReturn(zoneMock);
    }
}
