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

package org.apache.ignite.internal.cli.config;

import static java.util.stream.Collectors.toUnmodifiableSet;

import java.util.Set;

/** CLI config keys and constants. */
public enum CliConfigKeys {

    /** Default cluster or node URL property name. */
    CLUSTER_URL(Constants.CLUSTER_URL),

    /** Last connected cluster or node URL property name. */
    LAST_CONNECTED_URL(Constants.LAST_CONNECTED_URL),

    /** REST trust store path property name. */
    REST_TRUST_STORE_PATH(Constants.REST_TRUST_STORE_PATH),

    /** REST trust store password property name. */
    REST_TRUST_STORE_PASSWORD(Constants.REST_TRUST_STORE_PASSWORD),

    /** REST key store path property name. */
    REST_KEY_STORE_PATH(Constants.REST_KEY_STORE_PATH),

    /** REST key store password property name. */
    REST_KEY_STORE_PASSWORD(Constants.REST_KEY_STORE_PASSWORD),

    /** REST SSL ciphers property name. */
    REST_CIPHERS(Constants.REST_CIPHERS),

    /** Default JDBC URL property name. */
    JDBC_URL(Constants.JDBC_URL),

    /** JDBC SSL enabled property name. */
    JDBC_SSL_ENABLED(Constants.JDBC_SSL_ENABLED),

    /** JDBC trust store path property name. */
    JDBC_TRUST_STORE_PATH(Constants.JDBC_TRUST_STORE_PATH),

    /** JDBC trust store password property name. */
    JDBC_TRUST_STORE_PASSWORD(Constants.JDBC_TRUST_STORE_PASSWORD),

    /** JDBC key store path property name. */
    JDBC_KEY_STORE_PATH(Constants.JDBC_KEY_STORE_PATH),

    /** JDBC key store password property name. */
    JDBC_KEY_STORE_PASSWORD(Constants.JDBC_KEY_STORE_PASSWORD),

    /** JDBC SSL client auth property name. */
    JDBC_CLIENT_AUTH(Constants.JDBC_CLIENT_AUTH),

    /** JDBC SSL ciphers property name. */
    JDBC_CIPHERS(Constants.JDBC_CIPHERS),

    /** Basic authentication username. */
    BASIC_AUTHENTICATION_USERNAME(Constants.BASIC_AUTHENTICATION_USERNAME),

    /** Basic authentication password. */
    BASIC_AUTHENTICATION_PASSWORD(Constants.BASIC_AUTHENTICATION_PASSWORD),

    SQL_MULTILINE(Constants.SQL_MULTILINE),

    SYNTAX_HIGHLIGHTING(Constants.SYNTAX_HIGHLIGHTING),

    /** Pager enabled property name. */
    PAGER_ENABLED(Constants.PAGER_ENABLED),

    /** Pager command property name. */
    PAGER_COMMAND(Constants.PAGER_COMMAND),

    /** Output truncation enabled property name. */
    OUTPUT_TRUNCATE(Constants.OUTPUT_TRUNCATE),

    /** Maximum column width property name. */
    OUTPUT_MAX_COLUMN_WIDTH(Constants.OUTPUT_MAX_COLUMN_WIDTH),

    /** Color scheme property name (dark, light). */
    COLOR_SCHEME(Constants.COLOR_SCHEME);

    private final String value;

    public String value() {
        return value;
    }

    /**
     * Returns all secret config keys.
     */
    public static Set<String> secretConfigKeys() {
        return Set.of(
                        REST_KEY_STORE_PASSWORD,
                        REST_KEY_STORE_PATH,
                        REST_TRUST_STORE_PASSWORD,
                        REST_TRUST_STORE_PATH,
                        JDBC_SSL_ENABLED,
                        JDBC_KEY_STORE_PASSWORD,
                        JDBC_KEY_STORE_PATH,
                        JDBC_TRUST_STORE_PASSWORD,
                        JDBC_TRUST_STORE_PATH,
                        JDBC_CLIENT_AUTH,
                        BASIC_AUTHENTICATION_USERNAME,
                        BASIC_AUTHENTICATION_PASSWORD
                )
                .stream()
                .map(CliConfigKeys::value)
                .collect(toUnmodifiableSet());
    }

    /** Constants for CLI config. */
    public static final class Constants {
        public static final String CLUSTER_URL = "ignite.cluster-endpoint-url";

        public static final String LAST_CONNECTED_URL = "ignite.last-connected-url";

        public static final String REST_TRUST_STORE_PATH = "ignite.rest.trust-store.path";

        public static final String REST_TRUST_STORE_PASSWORD = "ignite.rest.trust-store.password";

        public static final String REST_KEY_STORE_PATH = "ignite.rest.key-store.path";

        public static final String REST_KEY_STORE_PASSWORD = "ignite.rest.key-store.password";

        public static final String REST_CIPHERS = "ignite.rest.ciphers";

        public static final String JDBC_URL = "ignite.jdbc-url";

        public static final String JDBC_SSL_ENABLED = "ignite.jdbc.ssl-enabled";

        public static final String JDBC_TRUST_STORE_PATH = "ignite.jdbc.trust-store.path";

        public static final String JDBC_TRUST_STORE_PASSWORD = "ignite.jdbc.trust-store.password";

        public static final String JDBC_KEY_STORE_PATH = "ignite.jdbc.key-store.path";

        public static final String JDBC_KEY_STORE_PASSWORD = "ignite.jdbc.key-store.password";

        public static final String JDBC_CLIENT_AUTH = "ignite.jdbc.client-auth";

        public static final String JDBC_CIPHERS = "ignite.jdbc.ciphers";

        public static final String BASIC_AUTHENTICATION_USERNAME = "ignite.auth.basic.username";

        public static final String BASIC_AUTHENTICATION_PASSWORD = "ignite.auth.basic.password";

        public static final String SQL_MULTILINE = "ignite.cli.sql.multiline";

        public static final String SYNTAX_HIGHLIGHTING = "ignite.cli.syntax-highlighting";

        public static final String PAGER_ENABLED = "ignite.cli.pager.enabled";

        public static final String PAGER_COMMAND = "ignite.cli.pager.command";

        public static final String OUTPUT_TRUNCATE = "ignite.cli.output.truncate";

        public static final String OUTPUT_MAX_COLUMN_WIDTH = "ignite.cli.output.max-column-width";

        public static final String COLOR_SCHEME = "ignite.cli.color-scheme";
    }

    CliConfigKeys(String value) {
        this.value = value;
    }
}
