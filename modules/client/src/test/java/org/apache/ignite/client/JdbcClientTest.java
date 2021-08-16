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

package org.apache.ignite.client;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.client.fakes.FakeIgniteQueryProcessor;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.QueryEventHandlerImpl;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.query.JdbcClientQueryEventHandler;
import org.apache.ignite.jdbc.ConnectionPropertiesImpl;
import org.apache.ignite.jdbc.JdbcConnection;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Jdbc client tests.
 */
public class JdbcClientTest extends AbstractClientTest {
    /**
     * Parameterized test source local and remote connections.
     */
    static List<JdbcConnection> connections() {
        ConnectionPropertiesImpl props = new ConnectionPropertiesImpl();

        JdbcQueryEventHandler hnd = new JdbcClientQueryEventHandler((TcpIgniteClient)client);

        var remoteCon = new JdbcConnection(hnd, props);

        JdbcQueryEventHandler processor = new QueryEventHandlerImpl(new FakeIgniteQueryProcessor());

        JdbcConnection locCon = new JdbcConnection(processor, props);

        return new ArrayList<>() {{
            add(remoteCon);
            add(locCon);
        }};
    }

    /**
     * Test array node change operation.
     */
    @ParameterizedTest
    @MethodSource("connections")
    public void statementTest(JdbcConnection conn) throws SQLException {
        Statement statement = conn.createStatement();

        ResultSet set = statement
            .executeQuery("SELECT * from TEST;");

        assertFalse(set.isLast());
        assertTrue(set.next());

        assertDoesNotThrow(() -> set.getInt(1));
        assertDoesNotThrow(() -> set.getLong(2));
        assertDoesNotThrow(() -> set.getFloat(3));
        assertDoesNotThrow(() -> set.getDouble(4));
        assertNotNull(set.getString(5));
        assertNull(set.getString(6));

        set.close();
    }

    /**
     * Test array node change operation.
     */
    @ParameterizedTest
    @MethodSource("connections")
    public void preparedStatementTest(JdbcConnection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("SELECT * from TEST;");

        ResultSet set = statement.executeQuery();

        assertFalse(set.isLast());
        assertTrue(set.next());

        assertDoesNotThrow(() -> set.getInt(1));
        assertDoesNotThrow(() -> set.getLong(2));
        assertDoesNotThrow(() -> set.getFloat(3));
        assertDoesNotThrow(() -> set.getDouble(4));
        assertNotNull(set.getString(5));
        assertNull(set.getString(6));

        set.close();
    }
}
