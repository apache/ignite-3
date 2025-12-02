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

package org.apache.ignite.client.handler.requests.tx;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_COMMIT_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientHandlerMetricSource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteTuple3;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.PendingTxPartitionEnlistment;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.tx.TransactionException;

/**
 * Client transaction commit request.
 */
public class ClientTransactionCommitRequest {
    /**
     * Processes the request.
     *
     * @param in Unpacker.
     * @param resources Resources.
     * @param metrics Metrics.
     * @param clockService Clock service.
     * @param igniteTables Tables.
     * @param enableDirectMapping Enable direct mapping flag.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(
            ClientMessageUnpacker in,
            ClientResourceRegistry resources,
            ClientHandlerMetricSource metrics,
            ClockService clockService,
            IgniteTablesInternal igniteTables,
            boolean enableDirectMapping,
            HybridTimestampTracker tsTracker
    ) throws IgniteInternalCheckedException {
        long resourceId = in.unpackLong();
        InternalTransaction tx = resources.remove(resourceId).get(InternalTransaction.class);

        // Attempt to merge server and client mappings.
        if (enableDirectMapping && !tx.isReadOnly()) {
            int cnt = in.unpackInt(); // Number of direct enlistments.

            List<IgniteTuple3<TablePartitionId, String, Long>> list = new ArrayList<>();
            for (int i = 0; i < cnt; i++) {
                int tableId = in.unpackInt();
                int partId = in.unpackInt();
                String consistentId = in.unpackString();
                long token = in.unpackLong();

                list.add(new IgniteTuple3<>(new TablePartitionId(tableId, partId), consistentId, token));
            }

            if (cnt > 0) {
                long causality = in.unpackLong();

                // Update causality.
                clockService.updateClock(HybridTimestamp.hybridTimestamp(causality));
            }

            Exception ex = null;

            for (IgniteTuple3<TablePartitionId, String, Long> enlistment : list) {
                TableViewInternal table = igniteTables.cachedTable(enlistment.get1().tableId());

                if (table == null) {
                    ex = new TransactionException(TX_COMMIT_ERR, "Table not found [id=" + enlistment.get1().tableId() + ']');
                    break;
                }

                if (!merge(table.internalTable(), enlistment.get1().partitionId(), enlistment.get2(), enlistment.get3(), tx, true)) {
                    ex = new TransactionException(TX_COMMIT_ERR, "Invalid enlistment token [id=" + enlistment.get1().tableId() + ']');
                    break;
                }
            }

            if (ex != null) {
                // Perform enlistment for rollback.
                for (IgniteTuple3<TablePartitionId, String, Long> enlistment : list) {
                    TableViewInternal table = igniteTables.cachedTable(enlistment.get1().tableId());

                    if (table == null) {
                        continue;
                    }

                    merge(table.internalTable(), enlistment.get1().partitionId(), enlistment.get2(), enlistment.get3(), tx, false);
                }

                Exception finalEx = ex;

                return tx.rollbackAsync().handle((res, err) -> {
                    if (err != null) {
                        finalEx.addSuppressed(err);
                    }

                    metrics.transactionsActiveDecrement();

                    ExceptionUtils.sneakyThrow(finalEx);

                    return null;
                });
            }
        }

        return tx.commitAsync().handle((res, err) -> {
            if (!tx.isReadOnly()) {
                tsTracker.update(clockService.current());
            }

            metrics.transactionsActiveDecrement();

            if (err != null) {
                throw ExceptionUtils.sneakyThrow(err);
            }

            return null;
        });
    }

    /**
     * Attempts to merge server and client enlistments.
     *
     * @param table The table.
     * @param partId Partition id.
     * @param consistentId Node consistent id.
     * @param token Enlistment token.
     * @param tx The transaction.
     * @param commit Merge mode: commit (with validation) or rollback (no validations).
     *
     * @return {@code True} if merged.
     */
    public static boolean merge(InternalTable table, int partId, String consistentId, long token, InternalTransaction tx, boolean commit) {
        ZonePartitionId replicationGroupId = table.targetReplicationGroupId(partId);
        PendingTxPartitionEnlistment existing = tx.enlistedPartition(replicationGroupId);
        if (existing == null) {
            tx.enlist(replicationGroupId, table.tableId(), consistentId, token);
        } else {
            // Enlistment tokens should be equal on commit.
            return !commit || existing.consistencyToken() == token;
        }

        return true;
    }
}
