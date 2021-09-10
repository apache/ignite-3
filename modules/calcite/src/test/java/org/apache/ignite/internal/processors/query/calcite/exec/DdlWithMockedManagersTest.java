/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryProcessor;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.schema.PrimaryIndex.PRIMARY_KEY_INDEX_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** */
@ExtendWith({MockitoExtension.class, WorkDirectoryExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class DdlWithMockedManagersTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(DdlWithMockedManagersTest.class);

    /** The name of the table which is statically configured. */
    private static final String STATIC_TABLE_NAME = "t_stat";

    /** Node name. */
    private static final String NODE_NAME = "node1";

    /** Raft manager. */
    @Mock(lenient = true)
    private Loza rm;

    /** */
    @Mock(lenient = true)
    private ClusterService cs;

    /** MetaStorage manager. */
    @Mock(lenient = true)
    private MetaStorageManager mm;

    /** Affinity manager. */
    @Mock(lenient = true)
    private AffinityManager am;

    /** Schema manager. */
    @Mock(lenient = true)
    private SchemaManager sm;

    /** Table partitions. */
    private static final int PARTITIONS = 32;

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.";

    /** Public prefix for metastorage. */
    private static final String PUBLIC_PREFIX = "dst-cfg.table.tables.";

    /** Node configuration manager. */
    private ConfigurationManager nodeCfgMgr;

    /** Cluster configuration manager. */
    private ConfigurationManager clusterCfgMgr;

    @WorkDirectory
    private Path workDir;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
        UUID.randomUUID().toString(),
        NODE_NAME,
        new NetworkAddress("127.0.0.1", 2245)
    );

    /** Before all test scenarios. */
    @BeforeEach
    void setUp() {
        try {
            nodeCfgMgr = new ConfigurationManager(
                List.of(NodeConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of()
            );

            clusterCfgMgr = new ConfigurationManager(
                List.of(ClusterConfiguration.KEY, TablesConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(DISTRIBUTED),
                List.of()
            );

            nodeCfgMgr.start();
            clusterCfgMgr.start();

            nodeCfgMgr.bootstrap("{\n" +
                "   \"node\":{\n" +
                "      \"metastorageNodes\":[\n" +
                "         \"" + NODE_NAME + "\"\n" +
                "      ]\n" +
                "   }\n" +
                "}");

            clusterCfgMgr.bootstrap("{\n" +
                "   \"cluster\":{\n" +
                "   \"metastorageNodes\":[\n" +
                "      \"" + NODE_NAME + "\"\n" +
                "   ]\n" +
                "},\n" +
                "   \"table\":{\n" +
                "      \"tables\":{\n" +
                "         \"" + STATIC_TABLE_NAME + "\":{\n" +
                "            \"name\":\"TestTable\",\n" +
                "            \"partitions\":16,\n" +
                "            \"replicas\":1,\n" +
                "            \"columns\":{\n" +
                "               \"id\":{\n" +
                "                  \"name\":\"id\",\n" +
                "                  \"type\":{\n" +
                "                     \"type\":\"Int64\"\n" +
                "                  },\n" +
                "                  \"nullable\":false\n" +
                "               }\n" +
                "            },\n" +
                "            \"indices\":{\n" +
                "               \"pk\":{\n" +
                "                  \"name\":\"pk\",\n" +
                "                  \"type\":\"primary\",\n" +
                "                  \"uniq\":true,\n" +
                "                  \"columns\":{\n" +
                "                     \"id\":{\n" +
                "                        \"name\":\"id\",\n" +
                "                        \"asc\":true\n" +
                "                     }\n" +
                "                  }\n" +
                "               }\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "   }\n" +
                "}");
        }
        catch (Exception e) {
            LOG.error("Failed to bootstrap the test configuration manager.", e);

            fail("Failed to configure manager [err=" + e.getMessage() + ']');
        }
    }

    /** Stop configuration manager. */
    @AfterEach
    void tearDown() {
        nodeCfgMgr.stop();
        clusterCfgMgr.stop();
    }

    /** Returns current method name. */
    private static String getCurrentMethodName() {
        return StackWalker.getInstance()
            .walk(s -> s.skip(1).findFirst())
            .get()
            .getMethodName();
    }

    /**
     * Tests create a table through public API.
     */
    @Test
    public void testCreateTable() {
        TableManager tblManager = null;

        SqlQueryProcessor queryProc = null;

        try {
            tblManager = mockManagers();

            queryProc = new SqlQueryProcessor(cs, tblManager);

            SqlQueryProcessor finalQueryProc = queryProc;

            queryProc.start();

            String curMethodName = getCurrentMethodName();

            String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varbinary(255)) with partitions=1", curMethodName);

            queryProc.query("PUBLIC", newTblSql);

            assertTrue(tblManager.tables().stream().anyMatch(t -> t.tableName()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));

            String finalNewTblSql1 = newTblSql;

            assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC", finalNewTblSql1));

            newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))",
                " IF NOT EXISTS " + curMethodName);

            String finalNewTblSql2 = newTblSql;

            assertDoesNotThrow(() -> finalQueryProc.query("PUBLIC", finalNewTblSql2));
        }
        finally {
            Objects.requireNonNull(tblManager).stop();

            Objects.requireNonNull(queryProc).stop();
        }
    }

    /**
     * Tests create a table with multiple pk through public API.
     */
    @Test
    public void testCreateTableMultiplePk() {
        TableManager tblManager = null;

        SqlQueryProcessor queryProc = null;

        try {
            tblManager = mockManagers();

            queryProc = new SqlQueryProcessor(cs, tblManager);

            SqlQueryProcessor finalQueryProc = queryProc;

            queryProc.start();

            String curMethodName = getCurrentMethodName();

            String newTblSql = String.format("CREATE TABLE %s (c1 int, c2 int, c3 int, primary key(c1, c2))", curMethodName);

            queryProc.query("PUBLIC", newTblSql);

            assertTrue(tblManager.tables().stream().anyMatch(t -> t.tableName()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));
        } finally {
            Objects.requireNonNull(tblManager).stop();

            Objects.requireNonNull(queryProc).stop();
        }
    }

    /**
     * Tests create and drop table through public API.
     */
    @Test
    public void testDropTable() {
        TableManager tblManager = null;

        SqlQueryProcessor queryProc = null;

        try {
            tblManager = mockManagers();

            queryProc = new SqlQueryProcessor(cs, tblManager);

            SqlQueryProcessor finalQueryProc = queryProc;

            queryProc.start();

            String curMethodName = getCurrentMethodName();

            String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

            queryProc.query("PUBLIC", newTblSql);

            assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC",
                "DROP TABLE " + curMethodName + "_not_exist"));

            queryProc.query("PUBLIC", "DROP TABLE PUBLIC." + curMethodName);

            assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC",
                "DROP TABLE " + curMethodName));

            assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC",
                "DROP TABLE PUBLIC." + curMethodName));

            queryProc.query("PUBLIC", "DROP TABLE IF EXISTS PUBLIC." + curMethodName);

            assertTrue(tblManager.tables().stream().noneMatch(t -> t.tableName()
                .equalsIgnoreCase("PUBLIC." + curMethodName)));
        }
        finally {
            Objects.requireNonNull(tblManager).stop();

            Objects.requireNonNull(queryProc).stop();
        }
    }

    /**
     * Tests alter and drop columns through public API.
     */
    @Test
    public void testAlterAndDropSimpleCase() {
        TableManager tblManager = null;

        SqlQueryProcessor queryProc = null;

        try {
            AtomicReference<SchemaDescriptor> schemaDesc = new AtomicReference<>();

            tblManager = mockManagers(schemaDesc);

            queryProc = new SqlQueryProcessor(cs, tblManager);

            SqlQueryProcessor finalQueryProc = queryProc;

            queryProc.start();

            String curMethodName = getCurrentMethodName();

            String newTblSql = String.format("CREATE TABLE %s (c1 int PRIMARY KEY, c2 varchar(255))", curMethodName);

            queryProc.query("PUBLIC", newTblSql);

            String alterCmd = String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 int)", curMethodName);

            queryProc.query("PUBLIC", alterCmd);

            String alterIfExistsCmd = String.format("ALTER TABLE IF EXISTS %s ADD COLUMN (c3 varchar, c4 int)", curMethodName + "_NotExist");

            queryProc.query("PUBLIC", alterIfExistsCmd);

            String alterWithoutIfExistsCmd = String.format("ALTER TABLE %s ADD COLUMN (c3 varchar, c4 int)", curMethodName + "_NotExist");

            assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC", alterWithoutIfExistsCmd));

            assertTrue(Arrays.stream(schemaDesc.get().valueColumns().columns()).anyMatch(c -> "c3"
                .equalsIgnoreCase(c.name())));

            assertThrows(IgniteException.class, () -> finalQueryProc.query("PUBLIC", alterCmd));

            queryProc.query("PUBLIC", String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS c3 varchar", curMethodName));

            queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c3", curMethodName));

            queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN IF EXISTS c3", curMethodName));

            assertTrue(Arrays.stream(schemaDesc.get().valueColumns().columns()).anyMatch(c -> "c4"
                .equalsIgnoreCase(c.name())));

            queryProc.query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c4", curMethodName));

            assertFalse(Arrays.stream(schemaDesc.get().valueColumns().columns()).anyMatch(c -> "c4"
                .equalsIgnoreCase(c.name())));

            assertThrows(IgniteException.class, () -> finalQueryProc
                .query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN (c3, c4)", curMethodName)));

            assertThrows(IgniteException.class, () -> finalQueryProc
                .query("PUBLIC", String.format("ALTER TABLE %s DROP COLUMN c1", curMethodName)));
        }
        finally {
            Objects.requireNonNull(tblManager).stop();

            Objects.requireNonNull(queryProc).stop();
        }
    }

    /**
     * Instantiates a table and prepares Table manager.
     *
     * @return Table manager.
     */
    private TableManager mockManagers() {
        return mockManagers(new AtomicReference<>());
    }

    /**
     * Instantiates a table and prepares Table manager.
     *
     * @param schemaDesc Schema descriptor.
     * @return Table manager.
     */
    private TableManager mockManagers(@NotNull AtomicReference<SchemaDescriptor> schemaDesc) {
        when(rm.prepareRaftGroup(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        when(mm.hasMetastorageLocally(any())).thenReturn(true);

        when(cs.localConfiguration()).thenAnswer(invocation -> {
            ClusterLocalConfiguration ret = mock(ClusterLocalConfiguration.class);

            when(ret.getName()).thenReturn("node1");

            return ret;
        });

        when(cs.topologyService()).thenAnswer(invocation -> {
            TopologyService ret = mock(TopologyService.class);

            when(ret.localMember()).thenReturn(new ClusterNode("1", "node1", null));

            return ret;
        });

        when(cs.messagingService()).thenAnswer(invocation -> {
            MessagingService ret = mock(MessagingService.class);

            return ret;
        });

        CompletableFuture<UUID> tblIdFut = new CompletableFuture<>();

        AtomicReference<String> canonicalTableNameRef = new AtomicReference<>();

        AtomicBoolean tableCreatedFlag = new AtomicBoolean();

        when(mm.invoke(any(Condition.class), any(Operation.class), any(Operation.class))).thenAnswer(invocation -> {
            Condition condition = invocation.getArgument(0);

            String keyForCheck = canonicalTableNameRef.get() != null ?
                PUBLIC_PREFIX + ConfigurationUtil.escape(canonicalTableNameRef.get()) + ".name" : "";

            Object internalCondition = ReflectionUtils.tryToReadFieldValue(Condition.class, "cond", condition).get();

            Method getKeyMethod = ReflectionUtils.findMethod(internalCondition.getClass(), "key").get();

            String metastorageKey = new String((byte[])ReflectionUtils.invokeMethod(getKeyMethod, internalCondition));

            if (keyForCheck.equals(metastorageKey))
                return CompletableFuture.completedFuture(tableCreatedFlag.get());

            tblIdFut.complete(UUID.fromString(metastorageKey.substring(INTERNAL_PREFIX.length())));

            return CompletableFuture.completedFuture(true);
        });

        when(sm.initSchemaForTable(any(), any())).thenAnswer(ack -> {
            canonicalTableNameRef.set(ack.getArgument(1));

            return CompletableFuture.completedFuture(true);
        });

        when(sm.updateSchemaForTable(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(true));

        when(sm.unregisterSchemas(any())).thenReturn(CompletableFuture.completedFuture(true));

        TableManager tableManager = new TableManager(nodeCfgMgr, clusterCfgMgr, mm, sm, am, rm, workDir);

        tableManager.start();

        Answer smInitAndChangedAck = new Answer() {
            @Override public Object answer(InvocationOnMock invocation) {
                EventListener<SchemaEventParameters> schemaInitialized = invocation.getArgument(1);

                assertTrue(tblIdFut.isDone());

                SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);

                when(schemaRegistry.schema()).thenAnswer(ack -> schemaDesc.get());

                CompletableFuture.supplyAsync(() -> schemaInitialized.notify(
                    new SchemaEventParameters(tblIdFut.join(), schemaRegistry),
                    null));

                return null;
            }
        };

        doAnswer(smInitAndChangedAck).when(sm).listen(same(SchemaEvent.INITIALIZED), any());
        doAnswer(smInitAndChangedAck).when(sm).listen(same(SchemaEvent.CHANGED), any());

        when(am.calculateAssignments(any(), any())).thenReturn(CompletableFuture.completedFuture(true));
        when(am.removeAssignment(any())).thenReturn(CompletableFuture.completedFuture(true));

        doAnswer(invocation -> {
            EventListener<AffinityEventParameters> affinityCalculatedDelegate = invocation.getArgument(1);

            ArrayList<List<ClusterNode>> assignment = new ArrayList<>(PARTITIONS);

            for (int part = 0; part < PARTITIONS; part++)
                assignment.add(new ArrayList<>(Collections.singleton(node)));

            assertTrue(tblIdFut.isDone());

            CompletableFuture.supplyAsync(() -> affinityCalculatedDelegate.notify(
                new AffinityEventParameters(tblIdFut.join(), assignment),
                null));

            return null;
        }).when(am).listen(any(AffinityEvent.class), any());

        when(mm.range(eq(new ByteArray(PUBLIC_PREFIX)), any())).thenAnswer(invocation -> {
            Cursor<Entry> cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenReturn(false);

            return cursor;
        });

        clusterCfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().listen(ctx -> {
            final String canonicalTableName = canonicalTableNameRef.get();

            boolean createTbl = ctx.newValue().get(canonicalTableName) != null &&
                ctx.oldValue().get(canonicalTableName) == null;

            boolean dropTbl = ctx.oldValue().get(canonicalTableName) != null &&
                ctx.newValue().get(canonicalTableName) == null;

            if (!dropTbl) {
                TableView newNode = ctx.newValue().get(canonicalTableName);

                NamedListView<? extends IndexColumnView> idxCols = null;

                for (String idx : newNode.indices().namedListKeys()) {
                    if (newNode.indices().get(idx).name().equals(PRIMARY_KEY_INDEX_NAME)) {
                        idxCols = newNode.indices().get(idx).columns();
                        break;
                    }
                }

                assertNotNull(idxCols);

                var totalColsSize = newNode.columns().size();

                var intKeyCols = new org.apache.ignite.internal.schema.Column[idxCols.size()];
                var intValCols = new org.apache.ignite.internal.schema.Column[totalColsSize - idxCols.size()];

                int valPos = 0;
                int keyPos = 0;

                for (String newNodeKey : newNode.columns().namedListKeys()) {
                    ColumnView col0 = newNode.columns().get(newNodeKey);
                    var colName = col0.name();

                    org.apache.ignite.internal.schema.Column intCol0 =
                        new org.apache.ignite.internal.schema.Column(colName,
                            NativeTypes.from(convert(col0.type())), col0.nullable());

                    if (idxCols.get(newNodeKey) == null)
                        intValCols[valPos++] = intCol0;
                    else
                        intKeyCols[keyPos++] = intCol0;
                }

                schemaDesc.set(new SchemaDescriptor(UUID.randomUUID(), 1, intKeyCols, intValCols));
            }

            if (!createTbl && !dropTbl)
                return CompletableFuture.completedFuture(null);

            tableCreatedFlag.set(createTbl);

            when(mm.range(eq(new ByteArray(PUBLIC_PREFIX)), any())).thenAnswer(invocation -> {
                AtomicBoolean firstRecord = new AtomicBoolean(createTbl);

                Cursor<Entry> cursor = mock(Cursor.class);

                when(cursor.hasNext()).thenAnswer(hasNextInvocation ->
                    firstRecord.compareAndSet(true, false));

                Entry mockEntry = mock(Entry.class);

                when(mockEntry.key()).thenReturn(new ByteArray(PUBLIC_PREFIX + "uuid." + NamedListNode.NAME));

                when(mockEntry.value()).thenReturn(ByteUtils.toBytes(canonicalTableNameRef.get()));

                when(cursor.next()).thenReturn(mockEntry);

                return cursor;
            });

            return CompletableFuture.completedFuture(null);
        });

        return tableManager;
    }
}
