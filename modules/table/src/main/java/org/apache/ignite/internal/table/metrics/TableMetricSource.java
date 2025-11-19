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

package org.apache.ignite.internal.table.metrics;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.table.metrics.TableMetricSource.Holder;
import org.apache.ignite.table.QualifiedName;

/**
 * Set of metrics related to a specific table.
 *
 * <p>
 * <b>Metrics affected by key-value and record view operations:</b>
 * <table border="1">
 *   <caption>Methods and affected metrics</caption>
 *   <tr>
 *     <th>Method(s)</th>
 *     <th>RoReads</th>
 *     <th>RwReads</th>
 *     <th>Writes</th>
 *   </tr>
 *   <tr>
 *     <td>get, getOrDefault, contains</td>
 *     <td>Yes, for read-only transactions</td>
 *     <td>Yes, for read-write transactions</td>
 *     <td>No</td>
 *   </tr>
 *   <tr>
 *     <td>getAll, containsAll</td>
 *     <td>Yes, it is incremented by the number of keys read for read-only transactions</td>
 *     <td>Yes, it is incremented by the number of keys read for read-write transactions</td>
 *     <td>No</td>
 *   </tr>
 *   <tr>
 *     <td>put, upsert</td>
 *     <td>No</td>
 *     <td>No</td>
 *     <td>Yes</td>
 *   </tr>
 *   <tr>
 *     <td>putAll, upsertAll</td>
 *     <td>No</td>
 *     <td>No</td>
 *     <td>Yes, it is incremented by the number of keys inserted</td>
 *   </tr>
 *   <tr>
 *     <td>putIfAbsent, insert</td>
 *     <td>No</td>
 *     <td>Yes</td>
 *     <td>Yes, if the method returns true</td>
 *   </tr>
 *   <tr>
 *     <td>insertAll</td>
 *     <td>No</td>
 *     <td>Yes, it is incremented by the number of keys read, which is equal to the number of keys provided</td>
 *     <td>Yes, it is incremented by the number of keys inserted</td>
 *   </tr>
 *   <tr>
 *     <td>getAndPut, replace, getAndReplace</td>
 *     <td>No</td>
 *     <td>Yes</td>
 *     <td>Yes, if the value is inserted / replaced</td>
 *   </tr>
 *   <tr>
 *     <td>remove, delete</td>
 *     <td>No</td>
 *     <td>No</td>
 *     <td>Yes, if the method returns true</td>
 *   </tr>
 *   <tr>
 *     <td>removeAll, getAndRemove, deleteAll</td>
 *     <td>No</td>
 *     <td>No</td>
 *     <td>Yes, it is incremented by the number of keys removed</td>
 *   </tr>
 *   <tr>
 *     <td>conditional remove, deleteExact, deleteAllExact</td>
 *     <td>No</td>
 *     <td>Yes, it is incremented by the number of keys read, which is equal to the number of keys provided</td>
 *     <td>Yes, it is incremented by the number of keys removed</td>
 *   </tr>
 *   <tr>
 *     <td>getAndRemove</td>
 *     <td>No</td>
 *     <td>Yes</td>
 *     <td>Yes, if the value is removed</td>
 *   </tr>
 * </table>
 *
 * <i>Note: Only synchronous methods are listed. Asynchronous methods affect the same metrics.</i>
 */
public class TableMetricSource extends AbstractMetricSource<Holder> {
    /** Source name. */
    public static final String SOURCE_NAME = "tables";

    /** Metric names. */
    public static final String RO_READS = "RoReads";
    public static final String RW_READS = "RwReads";
    public static final String WRITES = "Writes";

    private final QualifiedName tableName;

    /**
     * Creates a new instance of {@link TableMetricSource}.
     *
     * @param tableName Qualified table name.
     */
    public TableMetricSource(QualifiedName tableName) {
        super(sourceName(tableName), "Table metrics.", "tables");
        this.tableName = tableName;
    }

    /**
     * Returns a metric source name for the given table name.
     *
     * @param tableName Qualified table name.
     * @return Metric source name.
     */
    public static String sourceName(QualifiedName tableName) {
        return SOURCE_NAME + '.' + tableName.toCanonicalForm();
    }

    /**
     * Returns the qualified name of the table.
     *
     * @return Qualified name of the table.
     */
    public QualifiedName qualifiedTableName() {
        return tableName;
    }

    /**
     * Increments a counter of reads.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     */
    public void onRead(boolean readOnly) {
        Holder holder = holder();

        if (holder != null) {
            if (readOnly) {
                holder.roReads.increment();
            } else {
                holder.rwReads.increment();
            }
        }
    }

    /**
     * Adds the given {@code x} to a counter of reads.
     *
     * @param readOnly {@code true} if read operation is executed within read-only transaction, and {@code false} otherwise.
     */
    public void onRead(int x, boolean readOnly) {
        Holder holder = holder();

        if (holder != null) {
            if (readOnly) {
                holder.roReads.add(x);
            } else {
                holder.rwReads.add(x);
            }
        }
    }

    /**
     * Increments a counter of writes.
     */
    public void onWrite() {
        Holder holder = holder();

        if (holder != null) {
            holder.writes.increment();
        }
    }

    /**
     * Adds the given {@code x} to a counter of writes.
     */
    public void onWrite(int x) {
        Holder holder = holder();

        if (holder != null) {
            holder.writes.add(x);
        }
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Actual metrics holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric roReads = new LongAdderMetric(
                RO_READS,
                "The total number of reads executed within read-write transactions.");

        private final LongAdderMetric rwReads = new LongAdderMetric(
                RW_READS,
                "The total number of reads executed within read-only transactions.");

        private final LongAdderMetric writes = new LongAdderMetric(
                WRITES,
                "The total number of writes executed within read-write transactions.");

        private final List<Metric> metrics = List.of(roReads, rwReads, writes);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
