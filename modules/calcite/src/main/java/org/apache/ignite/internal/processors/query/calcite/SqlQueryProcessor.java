package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;

import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.DummyPlanCache;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.processors.query.calcite.util.StripedThreadPoolExecutor;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SqlQueryProcessor {
    /** Default Ignite thread keep alive time. */
    public static final long DFLT_THREAD_KEEP_ALIVE_TIME = 60_000L;

    private final ExecutionService executionSrvc;
    private final MessageService msgSrvc;
    private final QueryTaskExecutor taskExecutor;

    public SqlQueryProcessor(
        ClusterService clusterSrvc,
        TableManager tableManager
    ) {
        taskExecutor = new QueryTaskExecutorImpl(
            new StripedThreadPoolExecutor(
                4,
                "calciteQry",
                null,
                true,
                DFLT_THREAD_KEEP_ALIVE_TIME
            )
        );

        msgSrvc = new MessageServiceImpl(
            clusterSrvc.topologyService(),
            clusterSrvc.messagingService(),
            taskExecutor
        );

        SchemaHolderImpl schemaHolder = new SchemaHolderImpl(clusterSrvc.topologyService());

        executionSrvc = new ExecutionServiceImpl<>(
            clusterSrvc.topologyService(),
            msgSrvc,
            new DummyPlanCache(),
            schemaHolder,
            taskExecutor,
            ArrayRowHandler.INSTANCE
        );

        tableManager.listen(TableEvent.CREATE, new TableCreatedListener(schemaHolder));
        tableManager.listen(TableEvent.ALTER, new TableUpdatedListener(schemaHolder));
        tableManager.listen(TableEvent.DROP, new TableDroppedListener(schemaHolder));
    }

    public List<Cursor<List<?>>> query(String schemaName, String qry, Object... params) {
        return executionSrvc.executeQuery(schemaName, qry, params);
    }

    private static abstract class AbstractTableEventListener implements EventListener<TableEventParameters> {
        protected final SchemaHolderImpl schemaHolder;

        public AbstractTableEventListener(
            SchemaHolderImpl schemaHolder
        ) {
            this.schemaHolder = schemaHolder;
        }

        /** {@inheritDoc} */
        @Override public void remove(@NotNull Throwable exception) {
            throw new IllegalStateException();
        }
    }

    private static class TableCreatedListener extends AbstractTableEventListener {
        public TableCreatedListener(
            SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeCreated(
                "PUBLIC",
                parameters.tableName(),
                parameters.table().schemaView().schema()
            );

            return false;
        }
    }

    private static class TableUpdatedListener extends AbstractTableEventListener {
        public TableUpdatedListener(
            SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeUpdated(
                "PUBLIC",
                parameters.tableName(),
                parameters.table().schemaView().schema()
            );

            return false;
        }
    }

    private static class TableDroppedListener extends AbstractTableEventListener {
        public TableDroppedListener(
            SchemaHolderImpl schemaHolder
        ) {
            super(schemaHolder);
        }

        /** {@inheritDoc} */
        @Override public boolean notify(@NotNull TableEventParameters parameters, @Nullable Throwable exception) {
            schemaHolder.onSqlTypeDropped(
                "PUBLIC",
                parameters.tableName()
            );

            return false;
        }
    }
}
