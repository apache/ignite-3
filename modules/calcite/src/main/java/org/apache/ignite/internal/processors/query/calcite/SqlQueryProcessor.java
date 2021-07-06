package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;

import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.DummyPlanCache;
import org.apache.ignite.internal.processors.query.calcite.schema.EmptySchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.util.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.ClusterService;

public class SqlQueryProcessor {
    /** Default Ignite thread keep alive time. */
    public static final long DFLT_THREAD_KEEP_ALIVE_TIME = 60_000L;

    private final ExecutionService executionSrvc;
    private final MessageService msgSrvc;
    private final QueryTaskExecutor taskExecutor;

    public SqlQueryProcessor(ClusterService clusterSrvc) {
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

        executionSrvc = new ExecutionServiceImpl<>(
            clusterSrvc.topologyService(),
            msgSrvc,
            new DummyPlanCache(),
            new EmptySchemaHolder(),
            taskExecutor,
            ArrayRowHandler.INSTANCE
        );
    }

    public List<Cursor<List<?>>> query(String schemaName, String qry, Object... params) {
        return executionSrvc.executeQuery(schemaName, qry, params);
    }
}
