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

package org.apache.ignite.cli.sql;

import jakarta.inject.Singleton;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory of {@link SqlManager}.
 */
@Singleton
public class SqlConnectionManagerFactory {
    private final Map<String, SqlManager> cache = new ConcurrentHashMap<>();

    /**
     * Create or get existed {@link SqlManager} for given jdbc url.
     *
     * @param jdbcUrl working jdbc url.
     * @return instance of {@link SqlManager}.
     */
    public SqlManager getManager(String jdbcUrl) {
        return cache.computeIfAbsent(jdbcUrl, s -> {
            try {
                return new SqlManager(s);
            } catch (SQLException e) {
                return null;
            }
        });
    }
}
