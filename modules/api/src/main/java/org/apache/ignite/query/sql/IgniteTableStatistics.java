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

package org.apache.ignite.query.sql;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Table statistics facade provides methods for managing table statistics.
 * <p>
 * Table statistics are used by SQL engine for SQL queries planning.
 */
public interface IgniteTableStatistics {
    /**
     * Get statistics info for table.
     *
     * @param tableName Table name.
     * @return Statistics info collection.
     */
    Collection<StatisticInfo> statistics(String tableName); //TODO: Local or global? Ready or in-progress? TBD.

    /**
     * Creates statistics for a table and initiate it gathering on the nodes.
     *
     * @param tableName Table name.
     * @param statisticsConfiguration Statistic configuration.
     * @return Operation future.
     */
    CompletableFuture<Void> gather(String tableName, Object statisticsConfiguration);
    //TODO: Creates new statistics in addition, or drop old and replaces with new?
    //TODO: Should the existed one be refreshed? or fail with exception?
    //TODO: What if existed statistics has different configuration?
    //TODO: Can future be cancelled?

    /**
     * Refresh a global statistic by given name or all existed statistics if no statistic name specified.
     *
     * @param tableName Table name.
     * @param statisticNames Statistic names (optional).
     * @return Operation future.
     */
    CompletableFuture<Void> refresh(String tableName, String... statisticNames);
    //TODO: What if statistics with name is not exists? one of names?

    /**
     * Drop table statistic.
     *
     * @param tableName Table name.
     * @param statisticNames Statistic names.     //TODO: Can be empty?
     */
    void drop(String tableName, String... statisticNames);

    /**
     * Refresh local statistic.
     *
     * @param tableName Table name.
     * @param statisticNames Statistic names.
     */
    void refreshLocal(String tableName, String... statisticNames); //TODO: Actually, drops local statistics to be automatically refreshed.

    //TODO TBD.
    interface StatisticInfo {
        String name();
        Object config();
    }
}
