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

package org.apache.ignite.jdbc;

import static org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl.URL_PREFIX;

import com.google.auto.service.AutoService;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.JdbcConnection;

/**
 * JDBC driver implementation for Apache Ignite 3.x.
 *
 * <p>Driver allows to get distributed data from Ignite 3 Data Storage using standard SQL queries and standard JDBC API.</p>
 * <p><h2>Register the JDBC drivers</h2></p>
 * <p>
 * The JDBC driver registration is automatically done via the Java Standard Edition Service Provider mechanism.
 * Ignite JDBC driver implements this feature and it is automatically registered for case the JDBC driver jar presents in classpath.
 * </p>
 * <h2>URL Format</h2>
 * <p>
 * The JDBC Driver supports the following URL formats to establish a connection with an Ignite 3 Database.
 * </p>
 * jdbc:ignite:thin://host[:port][,host[:port][/schema][[?parameter1=value1][&parameter2=value2],...]]
 * <p> or </p>
 * jdbc:ignite:thin://host[:port][,host[:port][/schema][[?parameter1=value1][;parameter2=value2],...]]
 * <p>
 * URL can have an optional list of name-value pairs as parameters after the '<B>?</B>' delimiter.
 * Name and value are separated by an '<B>=</B>' and multiple properties are separated either by an '<B>&</B>' or a '<B>;</B>'.
 * Separate sign can't be mixed and should be either semicolon or ampersand sign.
 * </p>
 *   <b>The list of supported name value pairs: </b><br>
 *   <table border="2">
 *   <tr>
 *   <th>Parameter Name</th>
 *   <th>Description</th>
 *   </tr>
 *   <tr>
 *   <td>schema</td>
 *   <td>Specifies default schema name</td>
 *   </tr>
 *   <tr>
 *   <td>queryTimeout</td>
 *   <td>Sets the number of seconds the driver will wait for a <code>Statement</code> object to execute. Zero means there is no limits.</td>
 *   </tr>
 *   <tr>
 *   <td>connectionTimeout</td>
 *   <td>Sets the number of milliseconds JDBC client will waits for server to response. Zero means there is no limits."</td>
 *   </tr>
 *   <tr>
 *   <td>reconnectThrottlingPeriod</td>
 *   <td>Sets the reconnect throttling period, in milliseconds. Zero means there is no limits.</td>
 *   </tr>
 *   <td>reconnectThrottlingRetries</td>
 *   <td>Sets the reconnect throttling retries. Zero means there is no limits.</td>
 *   </tr>
 *   <td>trustStorePath</td>
 *   <td>Path to trust store.</td>
 *   </tr>
 *   <td>trustStorePassword</td>
 *   <td>Trust store password</td>
 *   </tr>
 *   <td>keyStorePath</td>
 *   <td>Path to key store</td>
 *   </tr>
 *   <td>keyStorePassword</td>
 *   <td>Key store password</td>
 *   </tr>
 *   <td>clientAuth</td>
 *   <td>SSL client authentication.</td>
 *   </tr>
 *   <td>ciphers</td>
 *   <td>SSL ciphers.</td>
 *   </tr>
 *   <td>sslEnabled</td>
 *   <td>Enable ssl.</td>
 *   </tr>
 *   <td>username</td>
 *   <td>Username.</td>
 *   </tr>
 *   <td>password</td>
 *   <td>Password.</td>
 *   </tr>
 *   <td>connectionTimeZone</td>
 *   <td>Client connection time-zone ID.</td>
 *   </tr>
 * </table>
 * <p> As example: </p>
 * <b>jdbc:ignite:thin://192.168.1.1:10800/public?connectionTimeout=1000&sslEnabled=false?connectionTimeZone=GMT+1</b>
 * <p>
 * The driver follows the following precedence (high priority goes first) for parameter value resolution: API arguments (if it available on
 * <code>Connection</code> object), last instance in the connection string, properties object passed during connection.
 * </p>
 */
@AutoService(Driver.class)
public class IgniteJdbcDriver implements Driver {
    /** Driver instance. */
    private static Driver instance;

    static {
        register();
    }

    /** Major version. */
    private static final int MAJOR_VER = ProtocolVersion.LATEST_VER.major();

    /** Minor version. */
    private static final int MINOR_VER = ProtocolVersion.LATEST_VER.minor();

    /** {@inheritDoc} */
    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.init(url, props);

        return new JdbcConnection(connProps);
    }

    /** {@inheritDoc} */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(URL_PREFIX);
    }

    /** {@inheritDoc} */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.init(url, info);

        return connProps.getDriverPropertyInfo();
    }

    /** {@inheritDoc} */
    @Override
    public int getMajorVersion() {
        return MAJOR_VER;
    }

    /** {@inheritDoc} */
    @Override
    public int getMinorVersion() {
        return MINOR_VER;
    }

    /** {@inheritDoc} */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used.");
    }

    /**
     * Register the driver instance.
     *
     * @return Driver instance.
     * @throws RuntimeException when failed to register driver.
     */
    private static synchronized void register() {
        if (isRegistered()) {
            throw new RuntimeException("Driver is already registered. It can only be registered once.");
        }

        try {
            Driver registeredDriver = new IgniteJdbcDriver();
            DriverManager.registerDriver(registeredDriver);
            IgniteJdbcDriver.instance = registeredDriver;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Ignite JDBC driver.", e);
        }
    }

    /**
     * Checks if Driver is instantiated.
     *
     * @return {@code true} if the driver is registered against {@link DriverManager}.
     */
    private static boolean isRegistered() {
        return instance != null;
    }
}
