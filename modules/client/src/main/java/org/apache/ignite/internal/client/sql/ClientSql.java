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

package org.apache.ignite.internal.client.sql;

import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.Session;
import org.apache.ignite.sql.Session.SessionBuilder;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.sql.Statement.StatementBuilder;

/**
 * Client SQL.
 */
public class ClientSql implements IgniteSql {
    /** Channel. */
    private final ReliableChannel ch;

    /** Marshallers provider. */
    private final MarshallersProvider marshallers;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param marshallers Marshallers provider.
     */
    public ClientSql(ReliableChannel ch, MarshallersProvider marshallers) {
        this.ch = ch;
        this.marshallers = marshallers;
    }

    /** {@inheritDoc} */
    @Override
    public Session createSession() {
        return new ClientSession(ch, marshallers, null, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public SessionBuilder sessionBuilder() {
        return new ClientSessionBuilder(ch, marshallers);
    }

    /** {@inheritDoc} */
    @Override
    public Statement createStatement(String query) {
        return new ClientStatement(query, null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public StatementBuilder statementBuilder() {
        return new ClientStatementBuilder();
    }
}
