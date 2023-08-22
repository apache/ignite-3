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
import org.apache.ignite.tx.IgniteTransactions;

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
    public CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(SessionId sessionId, QueryContext context,
            IgniteTransactions transactions, String qry, Object... params) {

        Object[] unwrappedParams = Arrays.stream(params).map(NativeTypeWrapper::unwrap).toArray();

        return queryProcessor.querySingleAsync(sessionId, context, transactions, qry, unwrappedParams);
    }
}
