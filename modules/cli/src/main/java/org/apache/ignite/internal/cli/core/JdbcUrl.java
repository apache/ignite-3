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

package org.apache.ignite.internal.cli.core;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

/** Representation of Ignite JDBC url. */
public class JdbcUrl {
    private final String host;
    private final int port;

    /** Constructor. */
    private JdbcUrl(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** Returns {@link JdbcUrl} with provided host and port. */
    public static JdbcUrl of(String url, int port) throws MalformedURLException {
        return new JdbcUrl(new URL(url).getHost(), port);
    }

    @Override
    public String toString() {
        return "jdbc:ignite:thin://" + host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcUrl)) {
            return false;
        }
        JdbcUrl jdbcUrl = (JdbcUrl) o;
        return port == jdbcUrl.port && Objects.equals(host, jdbcUrl.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }
}
