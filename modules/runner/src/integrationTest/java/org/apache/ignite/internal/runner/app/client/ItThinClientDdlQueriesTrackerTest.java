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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.handler.ClientInboundMessageHandler;
import org.apache.ignite.client.handler.DdlBatchingSuggester;
import org.apache.ignite.internal.configuration.SuggestionsClusterExtensionConfiguration;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests to validate the behavior of the DDL queries suggestion handler.
 */
public class ItThinClientDdlQueriesTrackerTest extends ItAbstractThinClientTest {
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @Override
    protected int nodes() {
        return 1;
    }

    @AfterEach
    void tearDown() throws Exception {
        server(0).sql().executeScript("DROP TABLE IF EXISTS t");

        IgniteUtils.closeAll(closeables);
    }

    @Test
    void ddlIsTrackedByConnection() {
        server(0).sql().executeScript("CREATE TABLE t(id INT PRIMARY KEY)");

        IgniteClient client1 = client();
        ClientInboundMessageHandler handler1 = unwrapIgniteImpl(server(0)).clientInboundMessageHandler();

        // The handler "test reference" is updated after a new connection is established.
        IgniteClient client2 = startClient();
        ClientInboundMessageHandler handler2 = unwrapIgniteImpl(server(0)).clientInboundMessageHandler();

        assertThat(handler1, not(sameInstance(handler2)));

        addColumn(client1, 0);
        addColumn(client1, 1);

        assertThat(ddlQueriesInRow(handler1), is(2));
        assertThat(ddlQueriesInRow(handler2), is(0));

        addColumn(client2, 2);

        assertThat(ddlQueriesInRow(handler1), is(2));
        assertThat(ddlQueriesInRow(handler2), is(1));
    }

    @Test
    void disableSuggestion() {
        server(0).sql().executeScript("CREATE TABLE t(id INT PRIMARY KEY)");

        SuggestionsClusterExtensionConfiguration config = unwrapIgniteImpl(server(0)).clusterConfiguration()
                .getConfiguration(SuggestionsClusterExtensionConfiguration.KEY);

        { // Suggestion is enabled by default
            ClientInboundMessageHandler handler = unwrapIgniteImpl(server(0)).clientInboundMessageHandler();

            addColumn(client(), 0);
            assertThat(ddlQueriesInRow(handler), is(1));
        }

        { // Disable suggestion
            await(config.suggestions().sequentialDdlExecution().change(c -> c.changeEnabled(false)));

            // The handler "test reference" is updated after a new connection is established.
            IgniteClient client = startClient();
            ClientInboundMessageHandler handler = unwrapIgniteImpl(server(0)).clientInboundMessageHandler();

            addColumn(client, 1);
            addColumn(client, 2);
            assertThat(ddlQueriesInRow(handler), is(-1));
        }

        { // Enable suggestion
            await(config.suggestions().sequentialDdlExecution().change(c -> c.changeEnabled(true)));

            IgniteClient client = startClient();
            ClientInboundMessageHandler handler = unwrapIgniteImpl(server(0)).clientInboundMessageHandler();

            addColumn(client, 3);
            addColumn(client, 4);
            assertThat(ddlQueriesInRow(handler), is(2));
        }
    }

    private IgniteClient startClient() {
        IgniteClient client = IgniteClient.builder()
                .addresses(getClientAddresses().toArray(new String[0]))
                .build();

        closeables.add(client);

        return client;
    }

    private static void addColumn(IgniteClient client, int columnNumber) {
        String ddlQuery = IgniteStringFormatter.format("ALTER TABLE t ADD COLUMN col{} int;", columnNumber);

        client.sql().execute(null, ddlQuery).close();
    }

    private static int ddlQueriesInRow(ClientInboundMessageHandler handler) {
        Consumer<SqlQueryType> queryTypeListener = handler.queryTypeListener();

        if (queryTypeListener instanceof DdlBatchingSuggester) {
            return ((DdlBatchingSuggester) queryTypeListener).trackedQueriesCount();
        }

        return -1;
    }
}
