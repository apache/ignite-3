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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.Comparator.comparing;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.apache.ignite.internal.type.NativeTypes.STRING;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Supplier;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;

/** Helper class for disaster recovery system views. */
class DisasterRecoverySystemViews {
    private static final Comparator<GlobalTablePartitionState> GLOBAL_TABLE_PARTITION_STATE_COMPARATOR =
            comparing((GlobalTablePartitionState state) -> state.tableName).thenComparingInt(state -> state.partitionId);

    private static final Comparator<SystemViewLocalTablePartitionState> SYSTEM_VIEW_LOCAL_PARTITION_STATE_COMPARATOR =
            comparing((SystemViewLocalTablePartitionState state) -> state.state.tableName)
                    .thenComparingInt(state -> state.state.partitionId)
                    .thenComparing(state -> state.nodeName);

    private static final Comparator<GlobalPartitionState> GLOBAL_ZONE_PARTITION_STATE_COMPARATOR =
            comparing((GlobalPartitionState state) -> state.zoneName).thenComparingInt(state -> state.partitionId);

    private static final Comparator<SystemViewLocalZonePartitionState> SYSTEM_VIEW_LOCAL_ZONE_PARTITION_STATE_COMPARATOR =
            comparing((SystemViewLocalZonePartitionState state) -> state.state.zoneName)
                    .thenComparingInt(state -> state.state.partitionId)
                    .thenComparing(state -> state.nodeName);

    static SystemView<?> createGlobalTablePartitionStatesSystemView(DisasterRecoveryManager manager) {
        return SystemViews.<GlobalTablePartitionState>clusterViewBuilder()
                .name("GLOBAL_PARTITION_STATES")
                .addColumn("ZONE_NAME", STRING, state -> state.zoneName)
                .addColumn("TABLE_ID", INT32, state -> state.tableId)
                .addColumn("SCHEMA_NAME", STRING, state -> state.schemaName)
                .addColumn("TABLE_NAME", STRING, state -> state.tableName)
                .addColumn("PARTITION_ID", INT32, state -> state.partitionId)
                .addColumn("PARTITION_STATE", STRING, state -> state.state.name())
                .addColumn("ZONE_ID", INT32, state -> state.zoneId)
                .addColumn("SCHEMA_ID", INT32, state -> state.schemaId)
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .addColumn("STATE", STRING, state -> state.state.name())
                // End of legacy columns list. New columns must be added below this line.
                .dataProvider(systemViewPublisher(() -> globalTablePartitionStatesAsync(manager)))
                .build();
    }

    static SystemView<?> createLocalTablePartitionStatesSystemView(DisasterRecoveryManager manager) {
        return SystemViews.<SystemViewLocalTablePartitionState>clusterViewBuilder()
                .name("LOCAL_PARTITION_STATES")
                .addColumn("NODE_NAME", STRING, state -> state.nodeName)
                .addColumn("ZONE_NAME", STRING, state -> state.state.zoneName)
                .addColumn("TABLE_ID", INT32, state -> state.state.tableId)
                .addColumn("SCHEMA_NAME", STRING, state -> state.state.schemaName)
                .addColumn("TABLE_NAME", STRING, state -> state.state.tableName)
                .addColumn("PARTITION_ID", INT32, state -> state.state.partitionId)
                .addColumn("PARTITION_STATE", STRING, state -> state.state.state.name())
                .addColumn("ESTIMATED_ROWS", INT64, state -> state.state.estimatedRows)
                .addColumn("ZONE_ID", INT32, state -> state.state.zoneId)
                .addColumn("SCHEMA_ID", INT32, state -> state.state.schemaId)
                // TODO https://issues.apache.org/jira/browse/IGNITE-24589: Next columns are deprecated and should be removed.
                //  They are kept for compatibility with 3.0 version, to allow columns being found by their old names.
                .addColumn("STATE", STRING, state -> state.state.state.name())
                // End of legacy columns list. New columns must be added below this line.
                .dataProvider(systemViewPublisher(() -> localTablePartitionStatesAsync(manager)))
                .build();
    }

    static SystemView<?> createGlobalZonePartitionStatesSystemView(DisasterRecoveryManager manager) {
        return SystemViews.<GlobalPartitionState>clusterViewBuilder()
                .name("GLOBAL_ZONE_PARTITION_STATES")
                .addColumn("ZONE_NAME", STRING, state -> state.zoneName)
                .addColumn("PARTITION_ID", INT32, state -> state.partitionId)
                .addColumn("PARTITION_STATE", STRING, state -> state.state.name())
                .addColumn("ZONE_ID", INT32, state -> state.zoneId)
                .dataProvider(systemViewPublisher(() -> globalZonePartitionStatesAsync(manager)))
                .build();
    }

    static SystemView<?> createLocalZonePartitionStatesSystemView(DisasterRecoveryManager manager) {
        return SystemViews.<SystemViewLocalZonePartitionState>clusterViewBuilder()
                .name("LOCAL_ZONE_PARTITION_STATES")
                .addColumn("NODE_NAME", STRING, state -> state.nodeName)
                .addColumn("ZONE_NAME", STRING, state -> state.state.zoneName)
                .addColumn("PARTITION_ID", INT32, state -> state.state.partitionId)
                .addColumn("PARTITION_STATE", STRING, state -> state.state.state.name())
                .addColumn("ESTIMATED_ROWS", INT64, state -> state.state.estimatedRows)
                .addColumn("ZONE_ID", INT32, state -> state.state.zoneId)
                .dataProvider(systemViewPublisher(() -> localZonePartitionStatesAsync(manager)))
                .build();
    }

    private static <T> Publisher<T> systemViewPublisher(Supplier<CompletableFuture<Iterator<T>>> invokeApi) {
        return subscriber -> {
            CompletableFuture<Iterator<T>> invokeApiFuture = invokeApi.get();

            subscriber.onSubscribe(new SystemViewSubscription<>(subscriber, invokeApiFuture));
        };
    }

    private static CompletableFuture<Iterator<GlobalTablePartitionState>> globalTablePartitionStatesAsync(DisasterRecoveryManager manager) {
        return manager.globalTablePartitionStates(Set.of(), Set.of()).thenApply(states -> states.values().stream()
                .sorted(GLOBAL_TABLE_PARTITION_STATE_COMPARATOR)
                .iterator()
        );
    }

    private static CompletableFuture<Iterator<SystemViewLocalTablePartitionState>> localTablePartitionStatesAsync(
            DisasterRecoveryManager manager
    ) {
        return manager.localTablePartitionStates(Set.of(), Set.of(), Set.of()).thenApply(states -> states.values().stream()
                .flatMap(statesByNodeName -> statesByNodeName.entrySet().stream())
                .map(nodeStates -> new SystemViewLocalTablePartitionState(nodeStates.getKey(), nodeStates.getValue()))
                .sorted(SYSTEM_VIEW_LOCAL_PARTITION_STATE_COMPARATOR)
                .iterator()
        );
    }

    private static CompletableFuture<Iterator<GlobalPartitionState>> globalZonePartitionStatesAsync(DisasterRecoveryManager manager) {
        return manager.globalPartitionStates(Set.of(), Set.of()).thenApply(states -> states.values().stream()
                .sorted(GLOBAL_ZONE_PARTITION_STATE_COMPARATOR)
                .iterator()
        );
    }

    private static CompletableFuture<Iterator<SystemViewLocalZonePartitionState>> localZonePartitionStatesAsync(
            DisasterRecoveryManager manager
    ) {
        return manager.localPartitionStates(Set.of(), Set.of(), Set.of()).thenApply(states -> states.values().stream()
                .flatMap(statesByNodeName -> statesByNodeName.entrySet().stream())
                .map(nodeStates -> new SystemViewLocalZonePartitionState(nodeStates.getKey(), nodeStates.getValue()))
                .sorted(SYSTEM_VIEW_LOCAL_ZONE_PARTITION_STATE_COMPARATOR)
                .iterator()
        );
    }

    private static class SystemViewLocalTablePartitionState {
        private final String nodeName;

        private final LocalTablePartitionState state;

        private SystemViewLocalTablePartitionState(String nodeName, LocalTablePartitionState state) {
            this.nodeName = nodeName;
            this.state = state;
        }
    }

    private static class SystemViewLocalZonePartitionState {
        private final String nodeName;

        private final LocalPartitionState state;

        private SystemViewLocalZonePartitionState(String nodeName, LocalPartitionState state) {
            this.nodeName = nodeName;
            this.state = state;
        }
    }

    private static class SystemViewSubscription<T> implements Subscription {
        private final Subscriber<? super T> subscriber;

        private final CompletableFuture<Iterator<T>> invokeApiFuture;

        private volatile boolean complete;

        private SystemViewSubscription(Subscriber<? super T> subscriber, CompletableFuture<Iterator<T>> invokeApiFuture) {
            this.subscriber = subscriber;
            this.invokeApiFuture = invokeApiFuture;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                onError(new IllegalArgumentException("Must be positive: " + n));

                return;
            }

            if (complete) {
                return;
            }

            invokeApiFuture.whenComplete((iterator, throwable) -> {
                if (throwable != null) {
                    onError(throwable);
                } else {
                    drain(iterator, n);
                }
            });
        }

        @Override
        public void cancel() {
            complete = true;
        }

        private void onError(Throwable t) {
            cancel();
            subscriber.onError(t);
        }

        private void drain(Iterator<T> iterator, long n) {
            if (complete) {
                return;
            }

            while (iterator.hasNext() && n-- > 0) {
                subscriber.onNext(iterator.next());
            }

            if (!iterator.hasNext()) {
                complete = true;
                subscriber.onComplete();
            }
        }
    }
}
