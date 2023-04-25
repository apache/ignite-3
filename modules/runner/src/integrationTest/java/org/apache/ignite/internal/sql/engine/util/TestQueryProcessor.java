package org.apache.ignite.internal.sql.engine.util;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionInfo;

/**
 * {@link QueryProcessor} that handles test {@link NativeTypeWrapper native type wrappers} .
 */
public final class TestQueryProcessor implements QueryProcessor {

    private final QueryProcessor queryProcessor;

    public TestQueryProcessor(Ignite ignite) {
        this.queryProcessor = ((IgniteImpl) ignite).queryEngine();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        queryProcessor.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        queryProcessor.stop();
    }

    @Override
    public SessionId createSession(PropertiesHolder properties) {
        return queryProcessor.createSession(properties);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeSession(SessionId sessionId) {
        return queryProcessor.closeSession(sessionId);
    }

    /** {@inheritDoc} */
    @Override
    public List<SessionInfo> liveSessions() {
        return queryProcessor.liveSessions();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(SessionId sessionId, QueryContext context, String qry,
            Object... params) {

        Object[] unwrappedParams = Arrays.stream(params).map(NativeTypeWrapper::unwrap).toArray();

        return queryProcessor.querySingleAsync(sessionId, context, qry, unwrappedParams);
    }
}
