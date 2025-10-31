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

package org.apache.ignite.internal.jdbc;

import java.sql.SQLException;
import java.time.ZoneId;
import org.apache.ignite.internal.client.HostAndPort;

/**
 * Provide access and manipulations with connection JDBC properties.
 */
public interface ConnectionProperties {
    /**
     * Get the schema name.
     *
     * @return Schema name of the connection.
     */
    String getSchema();

    /**
     * Set the schema name.
     *
     * @param schema Schema name of the connection.
     */
    void setSchema(String schema);

    /**
     * Get the URL.
     *
     * @return The URL of the connection.
     */
    String getUrl();

    /**
     * Set the URL.
     *
     * @param url The URL of the connection.
     * @throws SQLException On invalid URL.
     */
    void setUrl(String url) throws SQLException;

    /**
     * Get the addresses.
     *
     * @return Ignite nodes addresses.
     */
    HostAndPort[] getAddresses();

    /**
     * Set the ignite node addresses.
     *
     * @param addrs Ignite nodes addresses.
     */
    void setAddresses(HostAndPort[] addrs);

    /**
     * Note: zero value means there is no limits.
     *
     * @return Query timeout in seconds.
     */
    Integer getQueryTimeout();

    /**
     * Note: zero value means there is no limits.
     *
     * @param qryTimeout Query timeout in seconds.
     * @throws SQLException On error.
     */
    void setQueryTimeout(Integer qryTimeout) throws SQLException;

    /**
     * Note: zero value means there is no limits.
     *
     * @return Connection timeout in milliseconds.
     */
    int getConnectionTimeout();

    /**
     * Note: zero value means there is no limits.
     *
     * @param connTimeout Connection timeout in milliseconds.
     * @throws SQLException On error.
     */
    void setConnectionTimeout(Integer connTimeout) throws SQLException;

    /**
     * SSL enabled.
     *
     * @return true if SSL is enabled.
     */
    boolean isSslEnabled();

    /**
     * Enable/disable ssl.
     *
     * @param enabled true if SSL is enabled.
     */
    void setSslEnabled(boolean enabled);

    /**
     * SSL ciphers.
     *
     * @param ciphers list of ciphers.
     */
    void setCiphers(String ciphers);

    /**
     * SSL ciphers.
     *
     * @return list of ciphers.
     */
    Iterable<String> getCiphers();

    /**
     * Set trust store path that will be used to setup SSL connection.
     *
     * @param trustStorePath Trust store path.
     */
    void setTrustStorePath(String trustStorePath);

    /**
     * Set trust store password.
     *
     * @param password Trust store password.
     */
    void setTrustStorePassword(String password);

    /**
     * Trust store path.
     *
     * @return Truststore path.
     */
    String getTrustStorePath();

    /**
     * Truststore password.
     *
     * @return Truststore password.
     */
    String getTrustStorePassword();

    /**
     * Set key store path that will be used to setup SSL connection.
     *
     * @param keyStorePath Key store path.
     */
    void setKeyStorePath(String keyStorePath);

    /**
     * Set key store password.
     *
     * @param password Key store password.
     */
    void setKeyStorePassword(String password);

    /**
     * Key store path.
     *
     * @return Keystore path.
     */
    String getKeyStorePath();

    /**
     * Keystore password.
     *
     * @return Keystore password.
     */
    String getKeyStorePassword();

    /**
     * Basic authentication username.
     *
     * @return Basic authentication username.
     */
    String getUsername();

    /**
     * Set username.
     *
     * @param username Username.
     */
    void setUsername(String username);

    /**
     * Password.
     *
     * @return Password.
     */
    String getPassword();

    /**
     * Set password.
     *
     * @param password Password.
     */
    void setPassword(String password);

    /**
     * Get connection time-zone ID.
     *
     * @return Connection time-zone ID.
     */
    ZoneId getConnectionTimeZone();

    /**
     * Set connection time-zone ID.
     *
     * @param timeZoneId Connection time-zone ID.
     */
    void setConnectionTimeZone(ZoneId timeZoneId);

    /**
     * Get the size of the partition awareness metadata cache.
     *
     * @return The size of the partition awareness metadata cache.
     */
    int getPartitionAwarenessMetadataCacheSize();
}
