package org.apache.ignite.internal.jdbc.proto;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.jdbc.proto.event.JdbcMetaColumnsResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryCloseResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchRequest;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryFetchResult;
import org.apache.ignite.internal.jdbc.proto.event.JdbcQueryMetadataRequest;

/**
 * Jdbc QUERY cursor operations handler interface.
 */
public interface JdbcQueryCursorHandler {
    /**
     * {@link JdbcQueryFetchRequest} command handler.
     *
     * @param req Fetch query request.
     * @return Result future.
     */
    CompletableFuture<JdbcQueryFetchResult> fetchAsync(JdbcQueryFetchRequest req);

    /**
     * {@link JdbcQueryCloseRequest} command handler.
     *
     * @param req Close query request.
     * @return Result future.
     */
    CompletableFuture<JdbcQueryCloseResult> closeAsync(JdbcQueryCloseRequest req);

    /**
     * {@link JdbcQueryMetadataRequest} command handler.
     *
     * @param req Jdbc query metadata request.
     * @return Result future.
     */
    CompletableFuture<JdbcMetaColumnsResult> queryMetadataAsync(JdbcQueryMetadataRequest req);
}
