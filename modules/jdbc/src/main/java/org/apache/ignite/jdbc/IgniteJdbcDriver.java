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
 *      <td>reconnectThrottlingPeriod</td>
 *      <td>Sets the reconnect throttling period, in milliseconds. Zero means there is no limits.
 *          <br>By default uses {@value org.apache.ignite.client.IgniteClientConfiguration#DFLT_RECONNECT_THROTTLING_PERIOD}.</td>
 *   </tr>
 *   <tr>
 *      <td>reconnectThrottlingRetries</td>
 *      <td>Sets the reconnect throttling retries. Zero means there is no limits.
 *          <br>By default uses {@value org.apache.ignite.client.IgniteClientConfiguration#DFLT_RECONNECT_THROTTLING_RETRIES}.</td>
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
