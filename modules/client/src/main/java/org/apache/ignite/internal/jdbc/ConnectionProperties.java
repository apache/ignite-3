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

package org.apache.ignite.internal.jdbc;

import java.sql.SQLException;
import org.apache.ignite.internal.client.HostAndPortRange;
import org.apache.ignite.internal.client.ProtocolBitmaskFeature;

//TODO IGNITE-15428 Refactor and implement SSL connection parameters.
/**
 * Provide access and manipulations with connection JDBC properties.
 */
public interface ConnectionProperties {
    /** SSL mode: DISABLE. */
    String SSL_MODE_DISABLE = "disable";

    /** SSL mode: REQUIRE. */
    String SSL_MODE_REQUIRE = "require";

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
    HostAndPortRange[] getAddresses();

    /**
     * Set the ignite node addresses.
     *
     * @param addrs Ignite nodes addresses.
     */
    void setAddresses(HostAndPortRange[] addrs);


    /**
     * Get the auto close server cursors flag.
     *
     * @return Auto close server cursors flag.
     */
    boolean isAutoCloseServerCursor();

    /**
     * Set the auto close server cursors flag.
     *
     * @param autoCloseServerCursor Auto close server cursors flag.
     */
    void setAutoCloseServerCursor(boolean autoCloseServerCursor);

    /**
     * Get the socket send buffer size.
     *
     * @return Socket send buffer size.
     */
    int getSocketSendBuffer();

    /**
     * Set the socket send buffer size.
     *
     * @param size Socket send buffer size.
     * @throws SQLException On error.
     */
    void setSocketSendBuffer(int size) throws SQLException;

    /**
     * Get the socket receive buffer size.
     *
     * @return Socket receive buffer size.
     */
    int getSocketReceiveBuffer();

    /**
     * Set the socket receive buffer size.
     *
     * @param size Socket receive buffer size.
     * @throws SQLException On error.
     */
    void setSocketReceiveBuffer(int size) throws SQLException;

    /**
     * Get the TCP no delay flag.
     *
     * @return TCP no delay flag.
     */
    boolean isTcpNoDelay();

    /**
     * Set the TCP no delay flag.
     *
     * @param tcpNoDelay TCP no delay flag.
     */
    void setTcpNoDelay(boolean tcpNoDelay);

    /**
     * Gets SSL connection mode.
     *
     * @return Use SSL flag.
     * @see #setSslMode(String)
     */
    String getSslMode();

    /**
     * Use SSL connection to Ignite node. In case set to {@code "require"} SSL context must be configured.
     * {@link #setSslClientCertificateKeyStoreUrl} property and related properties must be set up
     * or JSSE properties must be set up (see {@code javax.net.ssl.keyStore} and other {@code javax.net.ssl.*}
     * properties)
     *
     * In case set to {@code "disable"} plain connection is used.
     * Available modes: {@code "disable", "require"}. Default value is {@code "disable"}
     *
     * @param mode SSL mode.
     */
    void setSslMode(String mode);

    /**
     * Gets protocol for secure transport.
     *
     * @return SSL protocol name.
     */
    String getSslProtocol();

    /**
     * Sets protocol for secure transport. If not specified, TLS protocol will be used.
     * Protocols implementations supplied by JSEE: SSLv3 (SSL), TLSv1 (TLS), TLSv1.1, TLSv1.2
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param sslProtocol SSL protocol name.
     */
    void setSslProtocol(String sslProtocol);

    /**
     * Gets cipher suites.
     *
     * @return SSL cipher suites.
     */
    String getSslCipherSuites();

    /**
     * Override default cipher suites.
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param sslCipherSuites SSL cipher suites.
     */
     void setSslCipherSuites(String sslCipherSuites);

    /**
     * Gets algorithm that will be used to create a key manager.
     *
     * @return Key manager algorithm.
     */
    String getSslKeyAlgorithm();

    /**
     * Sets key manager algorithm that will be used to create a key manager. Notice that in most cased default value
     * suites well, however, on Android platform this value need to be set to X509.
     * Algorithms implementations supplied by JSEE: PKIX (X509 or SunPKIX), SunX509
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param keyAlgorithm Key algorithm name.
     */
    void setSslKeyAlgorithm(String keyAlgorithm);

    /**
     * Gets the key store URL.
     *
     * @return Client certificate KeyStore URL.
     */
    String getSslClientCertificateKeyStoreUrl();

    /**
     * Sets path to the key store file. This is a mandatory parameter since
     * ssl context could not be initialized without key manager.
     *
     * In case {@link #getSslMode()} is {@code required} and key store URL isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.keyStore} will be used.
     *
     * @param url Client certificate KeyStore URL.
     */
    void setSslClientCertificateKeyStoreUrl(String url);

    /**
     * Gets key store password.
     *
     * @return Client certificate KeyStore password.
     */
    String getSslClientCertificateKeyStorePassword();

    /**
     * Sets key store password.
     *
     * In case {@link #getSslMode()} is {@code required}  and key store password isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.keyStorePassword} will be used.
     *
     * @param passwd Client certificate KeyStore password.
     */
    void setSslClientCertificateKeyStorePassword(String passwd);

    /**
     * Gets key store type used for context creation.
     *
     * @return Client certificate KeyStore type.
     */
    String getSslClientCertificateKeyStoreType();

    /**
     * Sets key store type used in context initialization.
     *
     * In case {@link #getSslMode()} is {@code required} and key store type isn't specified by Ignite properties
     *  (e.g. at JDBC URL)the JSSE property {@code javax.net.ssl.keyStoreType} will be used.
     * In case both Ignite properties and JSSE properties are not set the default 'JKS' type is used.
     *
     * <p>See more at JSSE Reference Guide.
     *
     * @param ksType Client certificate KeyStore type.
     */
    void setSslClientCertificateKeyStoreType(String ksType);

    /**
     * Gets the trust store URL.
     *
     * @return Trusted certificate KeyStore URL.
     */
    String getSslTrustCertificateKeyStoreUrl();

    /**
     * Sets path to the trust store file. This is an optional parameter,
     * however one of the {@code setSslTrustCertificateKeyStoreUrl(String)}, {@link #setSslTrustAll(boolean)}
     * properties must be set.
     *
     * In case {@link #getSslMode()} is {@code required} and trust store URL isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.trustStore} will be used.
     *
     * @param url Trusted certificate KeyStore URL.
     */
    void setSslTrustCertificateKeyStoreUrl(String url);

    /**
     * Gets trust store password.
     *
     * @return Trusted certificate KeyStore password.
     */
    String getSslTrustCertificateKeyStorePassword();

    /**
     * Sets trust store password.
     *
     * In case {@link #getSslMode()} is {@code required} and trust store password isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.trustStorePassword} will be used.
     *
     * @param passwd Trusted certificate KeyStore password.
     */
    void setSslTrustCertificateKeyStorePassword(String passwd);

    /**
     * Gets trust store type.
     *
     * @return Trusted certificate KeyStore type.
     */
    String getSslTrustCertificateKeyStoreType();

    /**
     * Sets trust store type.
     *
     * In case {@link #getSslMode()} is {@code required} and trust store type isn't specified by Ignite properties
     * (e.g. at JDBC URL) the JSSE property {@code javax.net.ssl.trustStoreType} will be used.
     * In case both Ignite properties and JSSE properties are not set the default 'JKS' type is used.
     *
     * @param ksType Trusted certificate KeyStore type.
     */
    void setSslTrustCertificateKeyStoreType(String ksType);

    /**
     * Gets trust any server certificate flag.
     *
     * @return Trust all certificates flag.
     */
    boolean isSslTrustAll();

    /**
     * Sets to {@code true} to trust any server certificate (revoked, expired or self-signed SSL certificates).
     *
     * <p> Defaults is {@code false}.
     *
     * Note: Do not enable this option in production you are ever going to use
     * on a network you do not entirely trust. Especially anything going over the internet.
     *
     * @param trustAll Trust all certificates flag.
     */
    void setSslTrustAll(boolean trustAll);

    /**
     * Gets the class name of the custom implementation of the Factory&lt;SSLSocketFactory&gt;.
     *
     * @return Custom class name that implements Factory&lt;SSLSocketFactory&gt;.
     */
    String getSslFactory();

    /**
     * Sets the class name of the custom implementation of the Factory&lt;SSLSocketFactory&gt;.
     * If {@link #getSslMode()} is {@code required} and factory is specified the custom factory will be used
     * instead of JSSE socket factory. So, other SSL properties will be ignored.
     *
     * @param sslFactory Custom class name that implements Factory&lt;SSLSocketFactory&gt;.
     */
    void setSslFactory(String sslFactory);

    /**
     * Set the user name.
     *
     * @param name User name to authentication.
     */
    void setUsername(String name);

    /**
     * Get the user name.
     *
     * @return User name to authentication.
     */
    String getUsername();

    /**
     * Set the user password.
     *
     * @param passwd User's password.
     */
    void setPassword(String passwd);

    /**
     * Get the user password.
     *
     * @return User's password.
     */
    String getPassword();

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
     * Any JDBC features could be force disabled.
     * See {@link ProtocolBitmaskFeature}.
     * The string should contain enumeration of feature names, separated by the comma.
     *
     * @return disabled features.
     */
    String disabledFeatures();

    /**
     * Disable features.
     *
     * @param features Disabled features. See {@link ProtocolBitmaskFeature}.
     *      The string should contain enumeration of feature names, separated by the comma.
     */
    void disabledFeatures(String features);

    /**
     * Gets the class name of the custom implementation of the Factory&lt;Map&lt;String, String&gt;&gt;.
     *
     * This factory should return user attributes which can be used on server node.
     *
     * @return Custom class name that implements Factory&lt;Map&lt;String, String&gt;&gt;.
     */
    String getUserAttributesFactory();

    /**
     * Sets the class name of the custom implementation of the Factory&lt;Map&lt;String, String&gt;&gt;.
     *
     * This factory should return user attributes which can be used on server node.
     *
     * @param sslFactory Custom class name that implements Factory&lt;Map&lt;String, String&gt;&gt;.
     */
    void setUserAttributesFactory(String sslFactory);
}
