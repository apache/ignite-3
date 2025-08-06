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

import java.io.Serializable;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.internal.client.HostAndPort;
import org.apache.ignite.internal.jdbc.proto.SqlStateCode;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Holds JDBC connection properties.
 */
public class ConnectionPropertiesImpl implements ConnectionProperties, Serializable {
    /** URL prefix. */
    public static final String URL_PREFIX = "jdbc:ignite:thin://";

    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Prefix for property names. */
    public static final String PROP_PREFIX = "ignite.jdbc.";

    /** Property: schema. */
    private static final String PROP_SCHEMA = "schema";

    /** Connection URL. */
    private String url;

    /** Addresses. */
    private HostAndPort[] addrs;

    /** Schema name. Hidden property. Is used to set default schema name part of the URL. */
    private final StringProperty schema = new StringProperty(PROP_SCHEMA,
            "Schema name of the connection", "PUBLIC", null, false, null);

    /** Query timeout. */
    private final IntegerProperty qryTimeout = new IntegerProperty("queryTimeout",
            "Sets the number of seconds the driver will wait for a <code>Statement</code> object to execute."
                    + " Zero means there is no limits.",
            null, false, 0, Integer.MAX_VALUE);

    /** JDBC connection timeout. */
    private final IntegerProperty connTimeout = new IntegerProperty("connectionTimeout",
            "Sets the number of milliseconds JDBC client will waits for server to response."
                    + " Zero means there is no limits.",
            0L, false, 0, Integer.MAX_VALUE);

    /** Path to the truststore. */
    private final StringProperty trustStorePath = new StringProperty("trustStorePath",
            "Path to trust store", null, null, false, null);

    /** Truststore password. */
    private final StringProperty trustStorePassword = new StringProperty("trustStorePassword",
            "Trust store password", null, null, false, null);

    /** Path to the keystore. */
    private final StringProperty keyStorePath = new StringProperty("keyStorePath",
            "Path to key store", null, null, false, null);

    /** Keystore password. */
    private final StringProperty keyStorePassword = new StringProperty("keyStorePassword",
            "Key store password", null, null, false, null);

    /** SSL ciphers list. */
    private final StringProperty ciphers = new StringProperty("ciphers",
            "SSL ciphers", null, null, false, null);

    /** Enable SSL. */
    private final BooleanProperty sslEnabled = new BooleanProperty("sslEnabled",
            "Enable ssl", false, null, false, null);

    /** Username. */
    private final StringProperty username = new StringProperty("username",
            "Username", null, null, false, null);

    /** Password. */
    private final StringProperty password = new StringProperty("password",
            "Password", null, null, false, null);

    /** Client connection time-zone ID. This property can be used by the client to change the time zone of the "session" on the server. */
    private final TimeZoneProperty connectionTimeZone = new TimeZoneProperty("connectionTimeZone",
            "Client connection time-zone ID", ZoneId.systemDefault(), null, false, null);

    /** The size of the partition awareness metadata cache. */
    private final IntegerProperty partitionAwarenessMetadataCacheSize = new IntegerProperty("partitionAwarenessMetadataCacheSize",
            "Partition awareness metadata cache size", IgniteClientConfiguration.DFLT_SQL_PARTITION_AWARENESS_METADATA_CACHE_SIZE,
            false, 0, Integer.MAX_VALUE);

    /** Properties array. */
    private final ConnectionProperty[] propsArray = {
            qryTimeout, connTimeout, trustStorePath, trustStorePassword,
            sslEnabled, ciphers, keyStorePath, keyStorePassword,
            username, password, connectionTimeZone, partitionAwarenessMetadataCacheSize
    };

    /** {@inheritDoc} */
    @Override
    public String getSchema() {
        return schema.value();
    }

    /** {@inheritDoc} */
    @Override
    public void setSchema(String schema) {
        this.schema.setValue(schema);
    }

    /** {@inheritDoc} */
    @Override
    public String getUrl() {
        if (url != null) {
            return url;
        } else {
            if (ArrayUtils.nullOrEmpty(getAddresses())) {
                return null;
            }

            StringBuilder sbUrl = new StringBuilder(URL_PREFIX);

            HostAndPort[] addrs = getAddresses();

            for (int i = 0; i < addrs.length; i++) {
                if (i > 0) {
                    sbUrl.append(',');
                }

                sbUrl.append(addrs[i].toString());
            }

            if (!isEmpty(getSchema())) {
                sbUrl.append('/').append(getSchema());
            }

            return sbUrl.toString();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setUrl(String url) throws SQLException {
        this.url = url;

        init(url, new Properties());
    }

    /** {@inheritDoc} */
    @Override
    public HostAndPort[] getAddresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override
    public void setAddresses(HostAndPort[] addrs) {
        this.addrs = addrs;
    }

    /** {@inheritDoc} */
    @Override
    public Integer getQueryTimeout() {
        return qryTimeout.value();
    }

    /** {@inheritDoc} */
    @Override
    public void setQueryTimeout(@Nullable Integer timeout) throws SQLException {
        qryTimeout.setValue(timeout);
    }

    /** {@inheritDoc} */
    @Override
    public int getConnectionTimeout() {
        return connTimeout.value();
    }

    /** {@inheritDoc} */
    @Override
    public void setConnectionTimeout(@Nullable Integer timeout) throws SQLException {
        connTimeout.setValue(timeout);
    }

    /** {@inheritDoc} */
    @Override
    public void setTrustStorePath(String trustStorePath) {
        this.trustStorePath.setValue(trustStorePath);
    }

    /** {@inheritDoc} */
    @Override
    public void setTrustStorePassword(String password) {
        this.trustStorePassword.setValue(password);
    }

    /** {@inheritDoc} */
    @Override
    public String getTrustStorePath() {
        return trustStorePath.value();
    }

    /** {@inheritDoc} */
    @Override
    public String getTrustStorePassword() {
        return trustStorePassword.value();
    }

    /** {@inheritDoc} */
    @Override
    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath.setValue(keyStorePath);
    }

    /** {@inheritDoc} */
    @Override
    public void setKeyStorePassword(String password) {
        this.keyStorePassword.setValue(password);
    }

    /** {@inheritDoc} */
    @Override
    public String getKeyStorePath() {
        return keyStorePath.value();
    }

    /** {@inheritDoc} */
    @Override
    public String getKeyStorePassword() {
        return keyStorePassword.value();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isSslEnabled() {
        return sslEnabled.value();
    }

    /** {@inheritDoc} */
    @Override
    public void setSslEnabled(boolean enabled) {
        sslEnabled.setValue(enabled);
    }

    @Override
    public void setCiphers(String ciphers) {
        this.ciphers.setValue(ciphers);
    }

    @Override
    public Iterable<String> getCiphers() {
        String value = ciphers.value();
        return value != null ? Arrays.asList(value.split(",")) : null;
    }

    @Override
    public String getUsername() {
        return username.value();
    }

    @Override
    public void setUsername(String username) {
        this.username.setValue(username);
    }

    @Override
    public String getPassword() {
        return password.value();
    }

    @Override
    public void setPassword(String password) {
        this.password.setValue(password);
    }

    @Override
    public ZoneId getConnectionTimeZone() {
        return connectionTimeZone.value();
    }

    @Override
    public void setConnectionTimeZone(ZoneId timeZoneId) {
        connectionTimeZone.setValue(timeZoneId);
    }

    @Override
    public int getPartitionAwarenessMetadataCacheSize() {
        return partitionAwarenessMetadataCacheSize.value();
    }

    /**
     * Init connection properties.
     *
     * @param url   URL connection.
     * @param props Environment properties.
     * @throws SQLException On error.
     */
    public void init(String url, Properties props) throws SQLException {
        assert props != null;
        Properties props0 = (Properties) props.clone();

        if (!isEmpty(url)) {
            parseUrl(url, props0);
        }

        for (ConnectionProperty arr : propsArray) {
            arr.init(props0);
        }
    }

    /**
     * Validates and parses connection URL.
     *
     * @param url   URL.
     * @param props Properties.
     * @throws SQLException On error.
     */
    private void parseUrl(String url, Properties props) throws SQLException {
        if (isEmpty(url)) {
            throw new SQLException("URL cannot be null or empty.");
        }

        if (!url.startsWith(URL_PREFIX)) {
            throw new SQLException("URL must start with \"" + URL_PREFIX + "\"");
        }

        String nakedUrl = url.substring(URL_PREFIX.length()).trim();

        parseUrl0(nakedUrl, props);
    }

    /**
     * Parse naked URL (i.e. without {@link ConnectionPropertiesImpl#URL_PREFIX}).
     *
     * @param url   Naked URL.
     * @param props Properties.
     * @throws SQLException If failed.
     */
    private void parseUrl0(String url, Properties props) throws SQLException {
        // Determine mode - semicolon or ampersand.
        int semicolonPos = url.indexOf(';');
        int slashPos = url.indexOf('/');
        int queryPos = url.indexOf('?');

        boolean semicolonMode;

        if (semicolonPos == -1 && slashPos == -1 && queryPos == -1) {
            // No special char -> any mode could be used, choose semicolon for simplicity.
            semicolonMode = true;
        } else {
            if (semicolonPos != -1) { // Use semicolon mode if it appears earlier than slash or query.
                semicolonMode = (slashPos == -1 || semicolonPos < slashPos) && (queryPos == -1 || semicolonPos < queryPos);
            } else { // Semicolon is not found.
                semicolonMode = false;
            }
        }

        if (semicolonMode) {
            parseUrlWithSemicolon(url, props);
        } else {
            parseUrlWithQuery(url, props);
        }
    }

    /**
     * Parse URL in semicolon mode.
     *
     * @param url   Naked URL
     * @param props Properties.
     * @throws SQLException If failed.
     */
    private void parseUrlWithSemicolon(String url, Properties props) throws SQLException {
        int pathPartEndPos = url.indexOf(';');

        if (pathPartEndPos == -1) {
            pathPartEndPos = url.length();
        }

        String pathPart = url.substring(0, pathPartEndPos);

        String paramPart = null;

        if (pathPartEndPos > 0 && pathPartEndPos < url.length()) {
            paramPart = url.substring(pathPartEndPos + 1);
        }

        parseEndpoints(pathPart);

        if (!isEmpty(paramPart)) {
            parseParameters(paramPart, props, ";");
        }
    }

    /**
     * Parse URL in query mode.
     *
     * @param url   Naked URL
     * @param props Properties.
     * @throws SQLException If failed.
     */
    private void parseUrlWithQuery(String url, Properties props) throws SQLException {
        int pathPartEndPos = url.indexOf('?');

        if (pathPartEndPos == -1) {
            pathPartEndPos = url.length();
        }

        String pathPart = url.substring(0, pathPartEndPos);

        String paramPart = null;

        if (pathPartEndPos > 0 && pathPartEndPos < url.length()) {
            paramPart = url.substring(pathPartEndPos + 1);
        }

        String[] pathParts = pathPart.split("/");

        parseEndpoints(pathParts[0]);

        if (pathParts.length > 2) {
            throw new SQLException("Invalid URL format (only schema name is allowed in URL path parameter "
                    + "'host:port[/schemaName]'): " + this.url, SqlStateCode.CLIENT_CONNECTION_FAILED);
        }

        setSchema(pathParts.length == 2 ? pathParts[1] : null);

        if (!isEmpty(paramPart)) {
            parseParameters(paramPart, props, "&");
        }
    }

    /**
     * Parse endpoints.
     *
     * @param endpointStr Endpoint string.
     * @throws SQLException If failed.
     */
    private void parseEndpoints(String endpointStr) throws SQLException {
        String[] endpoints = endpointStr.split(",");

        if (endpoints.length > 0) {
            addrs = new HostAndPort[endpoints.length];
        }

        for (int i = 0; i < endpoints.length; ++i) {
            try {
                addrs[i] = HostAndPort.parse(endpoints[i],
                        IgniteClientConfiguration.DFLT_PORT,
                        "Invalid endpoint format (should be \"host:port\")");
            } catch (IgniteException e) {
                throw new SQLException(e.getMessage(), SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (addrs == null || addrs.length == 0 || addrs[0].host() == null || addrs[0].host().isEmpty()) {
            throw new SQLException("Host name is empty", SqlStateCode.CLIENT_CONNECTION_FAILED);
        }
    }

    /**
     * Validates and parses URL parameters.
     *
     * @param paramStr  Parameters string.
     * @param props     Properties.
     * @param delimChar Delimiter character.
     * @throws SQLException If failed.
     */
    private void parseParameters(String paramStr, Properties props, String delimChar) throws SQLException {
        StringTokenizer st = new StringTokenizer(paramStr, delimChar);

        boolean insideBrace = false;

        String key = null;
        String val = null;

        while (st.hasMoreTokens()) {
            String token = st.nextToken();

            if (!insideBrace) {
                int eqSymPos = token.indexOf('=');

                if (eqSymPos < 0) {
                    throw new SQLException("Invalid parameter format (should be \"key1=val1" + delimChar
                            + "key2=val2" + delimChar + "...\"): " + token);
                }

                if (eqSymPos == token.length()) {
                    throw new SQLException("Invalid parameter format (key and value cannot be empty): " + token);
                }

                key = token.substring(0, eqSymPos);
                val = token.substring(eqSymPos + 1);

                if (val.startsWith("{")) {
                    val = val.substring(1);

                    insideBrace = true;
                }
            } else {
                val += delimChar + token; // NOPMD
            }

            if (val.endsWith("}")) {
                insideBrace = false;

                val = val.substring(0, val.length() - 1);
            }

            if (val.contains("{") || val.contains("}")) {
                throw new SQLException("Braces cannot be escaped in the value. "
                        + "Please use the connection Properties for such values. [property=" + key + ']');
            }

            if (!insideBrace) {
                if (key.isEmpty() || val.isEmpty()) {
                    throw new SQLException("Invalid parameter format (key and value cannot be empty): " + token);
                }

                if (PROP_SCHEMA.equalsIgnoreCase(key)) {
                    setSchema(val);
                } else {
                    props.setProperty(PROP_PREFIX + key, val);
                }
            }
        }
    }

    /**
     * Property validator interface.
     */
    private interface PropertyValidator extends Serializable {
        /**
         * Validate property.
         *
         * @param val String representation of the property value to validate.
         * @throws SQLException On validation fails.
         */
        void validate(String val) throws SQLException;
    }

    /**
     * Connection property.
     */
    private abstract static class ConnectionProperty implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Name. */
        protected String name;

        /** Property description. */
        protected String desc;

        /** Default value. */
        protected Object dfltVal;

        /**
         * An array of possible values if the value may be selected from a particular set of values; otherwise null.
         */
        protected String[] choices;

        /** Required flag. */
        protected boolean required;

        /** Property validator. */
        protected PropertyValidator validator;

        /**
         * Constructor.
         *
         * @param name     Name.
         * @param desc     Description.
         * @param dfltVal  Default value.
         * @param choices  Possible values.
         * @param required {@code true} if the property is required.
         */
        ConnectionProperty(String name, String desc, Object dfltVal, String[] choices, boolean required) {
            this.name = name;
            this.desc = desc;
            this.dfltVal = dfltVal;
            this.choices = choices;
            this.required = required;
        }

        /**
         * Constructor.
         *
         * @param name      Name.
         * @param desc      Description.
         * @param dfltVal   Default value.
         * @param choices   Possible values.
         * @param required  {@code true} if the property is required.
         * @param validator Property validator.
         */
        ConnectionProperty(String name, String desc, Object dfltVal, String[] choices, boolean required,
                PropertyValidator validator) {
            this.name = name;
            this.desc = desc;
            this.dfltVal = dfltVal;
            this.choices = choices;
            this.required = required;
            this.validator = validator;
        }

        /**
         * Get the default value.
         *
         * @return Default value.
         */
        Object getDfltVal() {
            return dfltVal;
        }

        /**
         * Get the property name.
         *
         * @return Property name.
         */
        String getName() {
            return name;
        }

        /**
         * Get the array of possible values.
         *
         * @return Array of possible values if the value may be selected from a particular set of values; otherwise null
         */
        String[] choices() {
            return choices;
        }

        /**
         * Init properties.
         *
         * @param props Properties.
         * @throws SQLException On error.
         */
        void init(Properties props) throws SQLException {
            String strVal = props.getProperty(PROP_PREFIX + name);

            if (required && strVal == null) {
                throw new SQLException("Property '" + name + "' is required but not defined",
                        SqlStateCode.CLIENT_CONNECTION_FAILED);
            }

            if (validator != null) {
                validator.validate(strVal);
            }

            checkChoices(strVal);

            props.remove(name);

            init(strVal);
        }

        /**
         * Init property.
         *
         * @param str String representation of the property value.
         * @throws SQLException on error.
         */
        abstract void init(String str) throws SQLException;

        /**
         * Check the choices.
         *
         * @param strVal Checked value.
         * @throws SQLException On check error.
         */
        protected void checkChoices(String strVal) throws SQLException {
            if (strVal == null) {
                return;
            }

            if (choices != null) {
                for (String ch : choices) {
                    if (ch.equalsIgnoreCase(strVal)) {
                        return;
                    }
                }

                throw new SQLException("Invalid property value. [name=" + name + ", val=" + strVal
                        + ", choices=" + Arrays.toString(choices) + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
            }
        }

        /**
         * Get the string representation of the property value.
         *
         * @return String representation of the property value.
         */
        abstract String valueObject();

        /**
         * Get the driver property info.
         *
         * @return JDBC property info object.
         */
        DriverPropertyInfo getDriverPropertyInfo() {
            DriverPropertyInfo dpi = new DriverPropertyInfo(PROP_PREFIX + name, valueObject());

            dpi.choices = choices();
            dpi.required = required;
            dpi.description = desc;

            return dpi;
        }
    }

    /**
     * Number property.
     */
    private abstract static class NumberProperty extends ConnectionProperty {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Value. */
        protected Number val;

        /** Allowed value range. */
        private Number[] range;

        /**
         * Constructor.
         *
         * @param name     Name.
         * @param desc     Description.
         * @param dfltVal  Default value.
         * @param required {@code true} if the property is required.
         * @param min      Lower bound of allowed range.
         * @param max      Upper bound of allowed range.
         */
        NumberProperty(String name, String desc, Number dfltVal, boolean required, Number min, Number max) {
            super(name, desc, dfltVal, null, required);

            val = dfltVal;

            range = new Number[]{min, max};
        }

        /** {@inheritDoc} */
        @Override
        void init(String str) throws SQLException {
            if (str == null) {
                val = dfltVal != null ? (Number) dfltVal : null;
            } else {
                try {
                    setValue(parse(str));
                } catch (NumberFormatException e) {
                    throw new SQLException("Failed to parse int property [name=" + name
                            + ", value=" + str + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
                }
            }
        }

        /**
         * Parse the property string representation.
         *
         * @param str String value.
         * @return Number value.
         * @throws NumberFormatException On parse error.
         */
        protected abstract Number parse(String str) throws NumberFormatException;

        /** {@inheritDoc} */
        @Override
        String valueObject() {
            return val != null ? String.valueOf(val) : null;
        }

        /**
         * Set number property value.
         *
         * @param val Property value.
         * @throws SQLException On error.
         */
        void setValue(Number val) throws SQLException {
            if (range != null) {
                if (val.doubleValue() < range[0].doubleValue()) {
                    throw new SQLException("Property cannot be lower than " + range[0].toString() + " [name=" + name
                            + ", value=" + val.toString() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
                }

                if (val.doubleValue() > range[1].doubleValue()) {
                    throw new SQLException("Property cannot be upper than " + range[1].toString() + " [name=" + name
                            + ", value=" + val.toString() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED);
                }
            }

            this.val = val;
        }
    }

    /**
     * Integer property.
     */
    private static class IntegerProperty extends NumberProperty {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Constructor.
         *
         * @param name     Name.
         * @param desc     Description.
         * @param dfltVal  Default value.
         * @param required {@code true} if the property is required.
         * @param min      Lower bound of allowed range.
         * @param max      Upper bound of allowed range.
         */
        IntegerProperty(String name, String desc, Number dfltVal, boolean required, int min, int max) {
            super(name, desc, dfltVal, required, min, max);
        }

        /** {@inheritDoc} */
        @Override
        protected Number parse(String str) throws NumberFormatException {
            return Integer.parseInt(str);
        }

        /**
         * Get the property value.
         *
         * @return Property value.
         */
        Integer value() {
            return val != null ? val.intValue() : null;
        }
    }

    /**
     * Long property.
     */
    private static class LongProperty extends NumberProperty {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Constructor.
         *
         * @param name     Name.
         * @param desc     Description.
         * @param dfltVal  Default value.
         * @param required {@code true} if the property is required.
         * @param min      Lower bound of allowed range.
         * @param max      Upper bound of allowed range.
         */
        LongProperty(String name, String desc, Number dfltVal, boolean required, long min, long max) {
            super(name, desc, dfltVal, required, min, max);
        }

        /** {@inheritDoc} */
        @Override
        protected Number parse(String str) throws NumberFormatException {
            return Long.parseLong(str);
        }

        /**
         * Get the property value.
         *
         * @return Property value.
         */
        Long value() {
            return val != null ? val.longValue() : null;
        }
    }

    /**
     * String property.
     */
    private static class StringProperty extends ConnectionProperty {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Value. */
        private String val;

        /**
         * Constructor.
         *
         * @param name      Name.
         * @param desc      Description.
         * @param dfltVal   Default value.
         * @param choices   Possible values.
         * @param required  {@code true} if the property is required.
         * @param validator Property value validator.
         */
        StringProperty(String name, String desc, String dfltVal, String[] choices, boolean required,
                PropertyValidator validator) {
            super(name, desc, dfltVal, choices, required, validator);

            val = dfltVal;
        }

        /**
         * Set the property value.
         *
         * @param val Property value.
         */
        void setValue(String val) {
            this.val = val;
        }

        /**
         * Get the property value.
         *
         * @return Property value.
         */
        String value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override
        void init(String str) throws SQLException {
            if (validator != null) {
                validator.validate(str);
            }

            if (str == null) {
                val = (String) dfltVal;
            } else {
                val = str;
            }
        }

        /** {@inheritDoc} */
        @Override
        String valueObject() {
            return val;
        }
    }

    /**
     * Boolean property.
     */
    private static class BooleanProperty extends ConnectionProperty {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Value. */
        private boolean val;

        /**
         * Constructor.
         *
         * @param name      Name.
         * @param desc      Description.
         * @param dfltVal   Default value.
         * @param required  {@code true} if the property is required.
         * @param validator Property value validator.
         */
        BooleanProperty(String name, String desc, boolean dfltVal, String[] choices, boolean required,
                PropertyValidator validator) {
            super(name, desc, dfltVal, choices, required, validator);

            val = dfltVal;
        }

        /**
         * Set the property value.
         *
         * @param val Property value.
         */
        void setValue(boolean val) {
            this.val = val;
        }

        /**
         * Get the property value.
         *
         * @return Property value.
         */
        boolean value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override
        void init(String str) throws SQLException {
            if (validator != null) {
                validator.validate(str);
            }

            val = Boolean.parseBoolean(str);
        }

        /** {@inheritDoc} */
        @Override
        String valueObject() {
            return String.valueOf(val);
        }
    }

    /**
     * Time zone property.
     */
    private static class TimeZoneProperty extends ConnectionProperty {
        private static final long serialVersionUID = 0L;

        private ZoneId val;

        TimeZoneProperty(String name, String desc, ZoneId dfltVal, String[] choices, boolean required,
                PropertyValidator validator) {
            super(name, desc, dfltVal, choices, required, validator);

            val = dfltVal;
        }

        /** {@inheritDoc} */
        @Override
        void init(String str) throws SQLException {
            if (str == null) {
                val = dfltVal != null ? (ZoneId) dfltVal : null;

                return;
            }

            try {
                val = ZoneId.of(str);
            } catch (DateTimeException e) {
                throw new SQLException("Failed to set time zone property [value=" + str + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        /** Sets the property value. */
        void setValue(ZoneId val) {
            this.val = val;
        }

        /** Returns property value. */
        ZoneId value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override
        String valueObject() {
            return String.valueOf(val);
        }
    }

    /**
     * Get the driver properties.
     *
     * @return Driver's properties info array.
     */
    public DriverPropertyInfo[] getDriverPropertyInfo() {
        DriverPropertyInfo[] infos = new DriverPropertyInfo[propsArray.length];

        for (int i = 0; i < propsArray.length; ++i) {
            infos[i] = propsArray[i].getDriverPropertyInfo();
        }

        return infos;
    }

    /**
     * Check if this string is null, empty or blank line.
     *
     * @param str Examined string.
     * @return {@code True} if this string is null, empty or blank line.
     */
    private static boolean isEmpty(String str) {
        return str == null || str.isEmpty() || str.isBlank();
    }
}
