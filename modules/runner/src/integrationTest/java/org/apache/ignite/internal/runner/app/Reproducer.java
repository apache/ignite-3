package org.apache.ignite.internal.runner.app;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType.TemporalColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

public class Reproducer {
    /** Test node name. */
    private static final String NODE_NAME = Reproducer.class.getCanonicalName();

    private static final String TABLE_NAME_ALL_COLUMNS_SQL = "tbl_all_columns_sql";

    private static final String SCHEMA_NAME = "PUBLIC";

    /** Nodes bootstrap configuration. */
    private static final Map<String, String> nodesBootstrapCfg = Map.of(
            NODE_NAME, "{\n"
                    + "  \"clientConnector\":{\"port\": 10942,\"portRange\":1,\"idleTimeout\":3000,\""
                    + "sendServerExceptionStackTraceToClient\":true},"
                    + "  \"network\": {\n"
                    + "    \"port\":3344,\n"
                    + "    \"nodeFinder\": {\n"
                    + "      \"netClusterNodes\":[ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\", \"localhost:3347\" ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}"
    );

    /** Base path for all temporary folders. */
    private static final Path BASE_PATH = Path.of("target", "work", "Reproducer");

    /**
     * Entry point.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        IgniteUtils.deleteIfExists(BASE_PATH);
        Files.createDirectories(BASE_PATH);

        List<CompletableFuture<Ignite>> igniteFutures = nodesBootstrapCfg.entrySet().stream()
                .map(e -> {
                    String nodeName = e.getKey();
                    String config = e.getValue();

                    return IgnitionManager.start(nodeName, config, BASE_PATH.resolve(nodeName));
                })
                .collect(toList());

        String metaStorageNodeName = nodesBootstrapCfg.keySet().iterator().next();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNodeName)
                .metaStorageNodeNames(List.of(metaStorageNodeName))
                .clusterName("cluster")
                .build();

        IgnitionManager.init(initParameters);
        List<Ignite> startedNodes = igniteFutures.stream().map(CompletableFuture::join).collect(toList());

        createTables(startedNodes.get(0));
    }

    private static void createTables(Ignite node) {
        var keyCol = "key";
        int maxTimePrecision = TemporalColumnType.MAX_TIME_PRECISION;

        TableDefinition schTblAllSql = SchemaBuilders.tableBuilder(SCHEMA_NAME, TABLE_NAME_ALL_COLUMNS_SQL).columns(
                SchemaBuilders.column(keyCol, ColumnType.INT64).build(),
                SchemaBuilders.column("str", ColumnType.string()).asNullable(true).build(),
                SchemaBuilders.column("int8", ColumnType.INT8).asNullable(true).build(),
                SchemaBuilders.column("int16", ColumnType.INT16).asNullable(true).build(),
                SchemaBuilders.column("int32", ColumnType.INT32).asNullable(true).build(),
                SchemaBuilders.column("int64", ColumnType.INT64).asNullable(true).build(),
                SchemaBuilders.column("float", ColumnType.FLOAT).asNullable(true).build(),
                SchemaBuilders.column("double", ColumnType.DOUBLE).asNullable(true).build(),
                SchemaBuilders.column("uuid", ColumnType.UUID).asNullable(true).build(),
                SchemaBuilders.column("date", ColumnType.DATE).asNullable(true).build(),
                SchemaBuilders.column("time", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("time2", ColumnType.time(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("datetime2", ColumnType.datetime(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("timestamp2", ColumnType.timestamp(maxTimePrecision)).asNullable(true).build(),
                SchemaBuilders.column("blob", ColumnType.blob()).asNullable(true).build(),
                SchemaBuilders.column("decimal", ColumnType.decimal()).asNullable(true).build()
        ).withPrimaryKey(keyCol).build();

        await(((TableManager) node.tables()).createTableAsync(schTblAllSql.name(), tblCh ->
                SchemaConfigurationConverter.convert(schTblAllSql, tblCh)
                        .changeReplicas(1)
                        .changePartitions(10)
        ));

        Table tbl = node.tables().table(TABLE_NAME_ALL_COLUMNS_SQL);

        Tuple rec = Tuple.create()
                .set("KEY", 1L)
                .set("STR", "v-1")
                .set("INT8", (byte) 2)
                .set("INT16", (short) 3)
                .set("INT32", 4)
                .set("INT64", 5L)
                .set("FLOAT", 6.5f)
                .set("DOUBLE", 7.5d)
                .set("DATE", LocalDate.of(2023, 1, 18))
                .set("DATETIME", LocalDateTime.of(2023, 1, 18, 18, 9, 29))
                .set("TIMESTAMP", Instant.now())
                .set("UUID", UUID.randomUUID());

        tbl.recordView().insert( null, rec);

        var query = "select \"KEY\", \"STR\", \"INT8\", \"INT16\", \"INT32\", \"INT64\", \"FLOAT\", \"DOUBLE\", \"DATE\", \"TIME\", "
                + "\"DATETIME\", \"TIMESTAMP\", \"BLOB\", \"DECIMAL\", \"UUID\" from TBL_ALL_COLUMNS_SQL ORDER BY KEY";

        node.sql().createSession().execute(null, query);
    }
}
