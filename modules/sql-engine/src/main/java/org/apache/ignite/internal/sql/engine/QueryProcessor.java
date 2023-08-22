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

package org.apache.ignite.internal.sql.engine;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.session.SessionInfo;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * QueryProcessor interface.
 */
public interface QueryProcessor extends IgniteComponent {
    /**
     * Creates a session with given properties.
     *
     * @param properties Properties to store within a new session.
     * @return An identifier of a created session.
     */
    SessionId createSession(PropertiesHolder properties);

    /**
     * Closes the session with given id.
     *
     * <p>This method just return a completed future in case the session was already closed or never exists.
     *
     * @param sessionId An identifier of a session to close.
     * @return A future representing result of an operation.
     */
    CompletableFuture<Void> closeSession(SessionId sessionId);

    /**
     * Provide list of live sessions.
     *
     * <p>This method return the information is actual only on method invocation time.
     *
     * @return List of active sessions.
     */
    List<SessionInfo> liveSessions();

    /**
     * Execute the single statement query with given schema name and parameters.
     *
     * <p>If the query string contains more than one statement the IgniteException will be thrown.
     *
     * @param context User query context.
     * @param transactions Transactions facade.
     * @param qry Single statement SQL query.
     * @param params Query parameters.
     * @return Sql cursor.
     *
     * @throws IgniteException in case of an error.
     */
    CompletableFuture<AsyncSqlCursor<List<Object>>> querySingleAsync(
            SessionId sessionId,
            QueryContext context,
            IgniteTransactions transactions,
            String qry,
            Object... params
    );
}
