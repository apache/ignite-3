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
import static org.apache.ignite.internal.jdbc.proto.SqlStateCode.CLIENT_CONNECTION_FAILED;
import static org.apache.ignite.internal.util.ViewUtils.sync;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import com.google.auto.service.AutoService;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClientAuthenticator;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.SslConfiguration;
import org.apache.ignite.internal.client.ChannelValidator;
import org.apache.ignite.internal.client.HostAndPort;
import org.apache.ignite.internal.client.IgniteClientConfigurationImpl;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.jdbc.ConnectionProperties;
import org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.JdbcClientDatabaseMetadataHandler;
import org.apache.ignite.internal.jdbc.JdbcConnection;
import org.apache.ignite.internal.jdbc.JdbcDatabaseMetadata;
import org.apache.ignite.internal.jdbc.proto.JdbcDatabaseMetadataHandler;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC driver implementation for Apache Ignite 3.x.
 *
 * <p>Driver allows to get distributed data from Ignite 3 Data Storage using standard SQL queries and standard JDBC API.
 * <h2>Register the JDBC drivers</h2>
 *
 * <p>The JDBC driver registration is automatically done via the Java Standard Edition Service Provider mechanism.
 * Ignite JDBC driver implements this feature and it is automatically registered for case the JDBC driver jar presents in classpath.
 *
 * <h2>URL Format</h2>
 *
 * <p>The JDBC Driver supports the following URL formats to establish a connection with an Ignite 3 Database:
 * <ol>
 *     <li>{@code jdbc:ignite:thin://host[:port][,host[:port][/schema][[?parameter1=value1][&parameter2=value2],...]]}</li>
 *     <li>{@code jdbc:ignite:thin://host[:port][,host[:port][/schema][[?parameter1=value1][;parameter2=value2],...]]}</li>
 * </ol>
 * Where
 * <ul>
 *     <li>host, port - network address of database. IPV4 and IPV6 supports both.</li>
 *     <li>schema - define schema used by default on the connection.</li>
 *     <li>URL can have an optional list of name-value pairs as parameters after the '<B>?</B>' delimiter.
 *          Name and value are separated by an '<B>=</B>' and multiple properties are separated either by an '<B>&amp;</B>' or a '<B>;</B>'.
 *          <br>Separate sign can't be mixed and should be either semicolon or ampersand sign.</li>
 * </ul>
 *
 * <table border="1" >
 *     <caption><b>The list of supported name value pairs:</b></caption>
 *   <tr>
 *      <th>Parameter Name</th>
 *      <th>Description</th>
 *   </tr>
 *   <tr>
 *      <th colspan="2">Common properties</th>
 *   </tr>
 *   <tr>
 *     <td>connectionTimeZone</td>
 *     <td>Client connection time-zone ID. This property can be used by the client to change the time zone of the "session" on the server.
 *          <br>Affects the interpretation of dates in queries without specifying a time zone.
 *          <br>If not set then system default on client timezone will be used.</td>
 *   </tr>
 *   <tr>
 *      <td>queryTimeout</td>
 *      <td>Number of seconds the driver will wait for a <code>Statement</code> object to execute. Zero means there is no limits.
 *          <br>By default no any timeout.</td>
 *   </tr>
 *   <tr>
 *      <th colspan="2">Connection properties</th>
 *   </tr>
 *   <tr>
 *      <td>connectionTimeout</td>
 *      <td>Number of milliseconds JDBC client will waits for server to response. Zero means there is no limits.
 *          <br>By default no any timeout.</td>
 *   </tr>
 *   <tr>
 *       <th colspan="2">Basic authentication</th>
 *   </tr>
 *   <tr>
 *      <td>username</td>
 *      <td>Username to login by basic authentication.</td>
 *   </tr>
 *   <tr>
 *      <td>password</td>
 *      <td>Password to login by basic authentication.</td>
 *   </tr>
 *   <tr>
 *       <th colspan="2">SSL security</th>
 *   </tr>
 *   <tr>
 *      <td>sslEnabled</td>
 *      <td>Enable ssl. Possible values:
 *      <ul>
 *          <li>true - enable SSL</li>
 *          <li>false - disable SSL</li>
 *      </ul>
 *      <b>Below security properties applies only if it enabled.</b></td>
 *   </tr>
 *   <tr>
 *      <td>trustStorePath</td>
 *      <td>Path to trust store on client side.</td>
 *   </tr>
 *   <tr>
 *      <td>trustStorePassword</td>
 *      <td>Trust store password.</td>
 *   </tr>
 *   <tr>
 *      <td>keyStorePath</td>
 *      <td>Path to key store on client side.</td>
 *   </tr>
 *   <tr>
 *      <td>keyStorePassword</td>
 *      <td>Key store password.</td>
 *   </tr>
 *   <tr>
 *      <td>clientAuth</td>
 *      <td>SSL client authentication. Supported values:
 *          <ul>
 *              <li>NONE - Indicates that the client authentication will not be requested.</li>
 *              <li>OPTIONAL - Indicates that the client authentication will be requested.</li>
 *              <li>REQUIRE - Indicates that the client authentication will be requested and the connection will be closed if the client
 *                  certificate is not trusted.
 *          </ul>
 *      </td>
 *   </tr>
 *   <tr>
 *      <td>ciphers</td>
 *      <td>SSL ciphers list separated by comma '<b>,</b>'.</td>
 *   </tr>
 *  </table>
 * <br>The driver follows the following precedence (high priority goes first) for parameter value resolution: API arguments (if it available
 * on <code>Connection</code> object), last instance in the connection string, properties object passed during connection.
 * <br>As example:
 * <br><b>jdbc:ignite:thin://192.168.1.1:10800/public?connectionTimeout=1000&amp;username=root&amp;password=pwd
 * &amp;connectionTimeZone=GMT+1</b>
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

    /**
     * Tracker of the latest time observed by client.
     *
     * <p>All connections created by this driver use the same tracker.
     * This is done so that read-only transactions from different connections can observe changes made in other connections,
     * which in turn ensures visibility of changes when working through the jdbc connection pool.
     */
    private final HybridTimestampTracker observableTimeTracker = HybridTimestampTracker.atomicTracker(null);

    /** {@inheritDoc} */
    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();
        connProps.init(url, props);

        TcpIgniteClient client;
        try {
            client = createIgniteClient(connProps, observableTimeTracker);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to server", CLIENT_CONNECTION_FAILED, e);
        }

        JdbcDatabaseMetadataHandler eventHandler = new JdbcClientDatabaseMetadataHandler(client);

        return new JdbcConnection(client, eventHandler, connProps);
    }

    /** {@inheritDoc} */
    @Override
    public boolean acceptsURL(String url) {
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
     * @throws RuntimeException when failed to register driver.
     */
    private static synchronized void register() {
        if (isRegistered()) {
            throw new RuntimeException("Driver is already registered. It can only be registered once.");
        }

        try {
            Driver registeredDriver = new IgniteJdbcDriver();
            DriverManager.registerDriver(registeredDriver);
            instance = registeredDriver;
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

    private TcpIgniteClient createIgniteClient(
            ConnectionProperties connectionProperties,
            HybridTimestampTracker observableTimeTracker
    ) {
        String[] addresses = Arrays.stream(connectionProperties.getAddresses())
                .map(HostAndPort::toString)
                .toArray(String[]::new);

        int networkTimeout = connectionProperties.getConnectionTimeout();

        var cfg = new IgniteClientConfigurationImpl(
                null,
                addresses,
                networkTimeout,
                IgniteClientConfigurationImpl.DFLT_BACKGROUND_RECONNECT_INTERVAL,
                null,
                IgniteClientConfigurationImpl.DFLT_HEARTBEAT_INTERVAL,
                IgniteClientConfigurationImpl.DFLT_HEARTBEAT_TIMEOUT,
                new RetryLimitPolicy(),
                null,
                extractSslConfiguration(connectionProperties),
                false,
                extractAuthenticationConfiguration(connectionProperties),
                IgniteClientConfiguration.DFLT_OPERATION_TIMEOUT,
                connectionProperties.getPartitionAwarenessMetadataCacheSize(),
                JdbcDatabaseMetadata.DRIVER_NAME
        );

        ChannelValidator channelValidator = ctx -> {
            if (!ctx.isFeatureSupported(ProtocolBitmaskFeature.SQL_MULTISTATEMENT_SUPPORT)) {
                ClusterNode node = ctx.clusterNode();

                throw new IgniteClientConnectionException(
                        CONNECTION_ERR,
                        IgniteStringFormatter.format("Connection to node aborted, because the node does not support "
                                + "the feature required by the driver being used. Please refer to the documentation and use a compatible "
                                + "version of the JDBC driver to connect to this node "
                                + "[name={}, address={}, productVersion={}, driverVersion={}.{}]",
                                node.name(), node.address(), ctx.productVersion(), getMajorVersion(), getMinorVersion()),
                        null
                );
            }
        };

        return (TcpIgniteClient) sync(TcpIgniteClient.startAsync(
                cfg, observableTimeTracker, channelValidator));
    }

    private static @Nullable SslConfiguration extractSslConfiguration(ConnectionProperties connProps) {
        if (connProps.isSslEnabled()) {
            return SslConfiguration.builder()
                    .enabled(true)
                    .trustStorePath(connProps.getTrustStorePath())
                    .trustStorePassword(connProps.getTrustStorePassword())
                    .ciphers(connProps.getCiphers())
                    .keyStorePath(connProps.getKeyStorePath())
                    .keyStorePassword(connProps.getKeyStorePassword())
                    .build();
        } else {
            return null;
        }
    }

    private static @Nullable IgniteClientAuthenticator extractAuthenticationConfiguration(ConnectionProperties connProps) {
        String username = connProps.getUsername();
        String password = connProps.getPassword();
        if (username != null && password != null) {
            return BasicAuthenticator.builder()
                    .username(username)
                    .password(password)
                    .build();
        } else {
            return null;
        }
    }
}
