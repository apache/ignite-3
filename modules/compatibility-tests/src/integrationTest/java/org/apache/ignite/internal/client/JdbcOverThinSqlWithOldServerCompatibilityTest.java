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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.internal.CompatibilityTestBase;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that the JDBC client aborts the connection to a server that does not support the required feature.
 */
@ParameterizedClass
@MethodSource("serverVersions")
public class JdbcOverThinSqlWithOldServerCompatibilityTest extends CompatibilityTestBase {
    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        // No-op.
    }

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        // Keep old servers running.
        return false;
    }

    private static List<String> serverVersions() {
        return List.of("3.0.0", "3.1.0");
    }

    @Test
    void jdbcConnectionToTheOldServerIsRejected() {
        Throwable ex = assertThrows(SQLException.class,
                () -> DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + cluster.clientPort()),
                "Failed to connect to server"
        );

        Throwable cause = ex.getCause();

        assertThat(cause, instanceOf(IgniteClientConnectionException.class));

        IgniteClientConnectionException connectEx = (IgniteClientConnectionException) cause;

        String expectedDriverVer = Pattern.quote(IgniteProductVersion.CURRENT_VERSION.toString());

        assertThat(connectEx.getMessage(),
                matchesPattern("Connection to node aborted, because the node does not support the feature required "
                        + "by the driver being used\\. Please refer to the documentation and use a compatible version "
                        + "of the JDBC driver to connect to this node "
                        + "\\[nodeName=.+, nodeAddress=.+, nodeVersion=.+, driverVersion=" + expectedDriverVer + ']'));
        assertThat(connectEx.code(), is(Client.CONNECTION_ERR));
    }
}
