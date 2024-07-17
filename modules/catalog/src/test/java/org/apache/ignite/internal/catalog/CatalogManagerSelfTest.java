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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.CatalogManagerImpl.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.catalog.CatalogService.SYSTEM_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.SqlCommon;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Catalog manager self test.
 */
public class CatalogManagerSelfTest extends BaseCatalogManagerTest {
    private static final String SCHEMA_NAME = SqlCommon.DEFAULT_SCHEMA_NAME;

    @Test
    public void testEmptyCatalog() {
        CatalogSchemaDescriptor defaultSchema = manager.schema(SCHEMA_NAME, 1);

        assertNotNull(defaultSchema);
        assertSame(defaultSchema, manager.activeSchema(SCHEMA_NAME, clock.nowLong()));
        assertSame(defaultSchema, manager.schema(1));
        assertSame(defaultSchema, manager.schema(defaultSchema.id(), 1));
        assertSame(defaultSchema, manager.activeSchema(clock.nowLong()));

        int nonExistingVersion = manager.latestCatalogVersion() + 1;

        assertNull(manager.schema(nonExistingVersion));
        assertNull(manager.schema(defaultSchema.id(), nonExistingVersion));
        assertThrows(IllegalStateException.class, () -> manager.activeSchema(-1L));

        // Validate default schema.
        assertEquals(SCHEMA_NAME, defaultSchema.name());
        assertEquals(1, defaultSchema.id());
        assertEquals(0, defaultSchema.tables().length);
        assertEquals(0, defaultSchema.indexes().length);

        // Default distribution zone must exists.
        CatalogZoneDescriptor zone = latestActiveCatalog().defaultZone();

        assertEquals(DEFAULT_ZONE_NAME, zone.name());
        assertEquals(DEFAULT_PARTITION_COUNT, zone.partitions());
        assertEquals(DEFAULT_REPLICA_COUNT, zone.replicas());
        assertEquals(DEFAULT_FILTER, zone.filter());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjust());
        assertEquals(IMMEDIATE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleUp());
        assertEquals(INFINITE_TIMER_VALUE, zone.dataNodesAutoAdjustScaleDown());

        // System schema should exist.

        CatalogSchemaDescriptor systemSchema = manager.schema(SYSTEM_SCHEMA_NAME, 1);
        assertNotNull(systemSchema, "system schema");
        assertSame(systemSchema, manager.activeSchema(SYSTEM_SCHEMA_NAME, clock.nowLong()));
        assertSame(systemSchema, manager.schema(SYSTEM_SCHEMA_NAME, 1));
        assertSame(systemSchema, manager.schema(systemSchema.id(), 1));

        // Validate system schema.
        assertEquals(SYSTEM_SCHEMA_NAME, systemSchema.name());
        assertEquals(2, systemSchema.id());
        assertEquals(0, systemSchema.tables().length);
        assertEquals(0, systemSchema.indexes().length);

        assertThat(manager.latestCatalogVersion(), is(1));
    }

    @Test
    public void assignsSuccessiveCatalogVersions() {
        CompletableFuture<Integer> version1Future = manager.execute(TestCommand.ok());
        assertThat(version1Future, willCompleteSuccessfully());

        CompletableFuture<Integer> version2Future = manager.execute(TestCommand.ok());
        assertThat(version2Future, willCompleteSuccessfully());

        CompletableFuture<Integer> version3Future = manager.execute(TestCommand.ok());
        assertThat(version3Future, willCompleteSuccessfully());

        int firstVersion = version1Future.join();
        assertThat(version2Future.join(), is(firstVersion + 1));
        assertThat(version3Future.join(), is(firstVersion + 2));
    }

    @Test
    public void testNoInteractionsAfterStop() {
        clearInvocations(updateLog);

        int futureVersion = manager.latestCatalogVersion() + 1;

        CompletableFuture<Void> readyFuture = manager.catalogReadyFuture(futureVersion);
        assertFalse(readyFuture.isDone());

        ComponentContext componentContext = new ComponentContext();

        assertThat(manager.stopAsync(componentContext), willCompleteSuccessfully());

        verify(updateLog).stopAsync(componentContext);

        assertTrue(readyFuture.isDone());

        manager.execute(catalog -> null);
        manager.execute(List.of(catalog -> null));

        verifyNoMoreInteractions(updateLog);
    }

    @Test
    public void operationWillBeRetriedFiniteAmountOfTimes() {
        UpdateLog updateLogMock = mock(UpdateLog.class);

        ArgumentCaptor<OnUpdateHandler> updateHandlerCapture = ArgumentCaptor.forClass(OnUpdateHandler.class);

        doNothing().when(updateLogMock).registerUpdateHandler(updateHandlerCapture.capture());

        ComponentContext componentContext = new ComponentContext();

        when(updateLogMock.startAsync(componentContext)).thenReturn(nullCompletedFuture());
        when(updateLogMock.append(any())).thenReturn(CompletableFuture.completedFuture(true));

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLogMock, clockService);
        assertThat(manager.startAsync(componentContext), willCompleteSuccessfully());

        reset(updateLogMock);

        when(updateLogMock.append(any())).thenAnswer(invocation -> {
            // here we emulate concurrent updates. First of all, we return a future completed with "false"
            // as if someone has concurrently appended an update. Besides, in order to unblock manager and allow to
            // make another attempt, we must notify manager with the same version as in current attempt.
            VersionedUpdate updateFromInvocation = invocation.getArgument(0, VersionedUpdate.class);

            VersionedUpdate update = new VersionedUpdate(
                    updateFromInvocation.version(),
                    updateFromInvocation.delayDurationMs(),
                    List.of(new ObjectIdGenUpdateEntry(1))
            );

            updateHandlerCapture.getValue().handle(update, clock.now(), 0);

            return falseCompletedFuture();
        });

        CompletableFuture<?> fut = manager.execute(List.of(TestCommand.ok()));

        assertThat(fut, willThrow(IgniteInternalException.class, "Max retry limit exceeded"));

        // retry limit is hardcoded at org.apache.ignite.internal.catalog.CatalogServiceImpl.MAX_RETRY_COUNT
        verify(updateLogMock, times(10)).append(any());
    }

    @Test
    public void catalogActivationTime() {
        delayDuration.set(TimeUnit.DAYS.toMillis(365));
        reset(updateLog, clockWaiter);

        Catalog initialCatalog = manager.catalog(manager.latestCatalogVersion());
        assertNotNull(initialCatalog);
        int initial = initialCatalog.objectIdGenState();

        CompletableFuture<Integer> createTableFuture = manager.execute(TestCommand.ok());

        assertFalse(createTableFuture.isDone());

        verify(updateLog).append(any());
        // TODO IGNITE-19400: recheck createTable future completion guarantees

        // This waits till the new Catalog version lands in the internal structures.
        verify(clockWaiter, timeout(10_000)).waitFor(any());

        int latestVersion = manager.latestCatalogVersion();

        assertSame(manager.schema(latestVersion - 1), manager.activeSchema(clock.nowLong()));
        Catalog latestCatalog = manager.catalog(manager.latestCatalogVersion());
        assertNotNull(latestCatalog);
        assertEquals(initial + 1, latestCatalog.objectIdGenState());

        clock.update(clock.now().addPhysicalTime(delayDuration.get()));

        Catalog latestCatalog2 = manager.catalog(latestVersion);
        assertNotNull(latestCatalog2);
        assertEquals(latestCatalog.objectIdGenState(), latestCatalog2.objectIdGenState());
    }

    @Test
    public void alwaysWaitForActivationTime() throws Exception {
        delayDuration.set(TimeUnit.DAYS.toMillis(365));
        partitionIdleSafeTimePropagationPeriod.set(0);
        reset(updateLog);

        CatalogCommand catalogCommand = spy(TestCommand.ok());

        int initialVersion = manager.latestCatalogVersion();

        CompletableFuture<Integer> createTableFuture1 = manager.execute(catalogCommand);

        // we should wait until command will be applied to catalog to avoid races
        // on next command execution
        await(manager.catalogReadyFuture(initialVersion + 1));

        assertFalse(createTableFuture1.isDone());

        ArgumentCaptor<VersionedUpdate> appendCapture = ArgumentCaptor.forClass(VersionedUpdate.class);

        verify(updateLog).append(appendCapture.capture());

        int catalogVerAfterTableCreate = appendCapture.getValue().version();

        CompletableFuture<Integer> commandFuture = manager.execute(catalogCommand);

        verify(catalogCommand, times(2)).get(any());

        assertFalse(commandFuture.isDone());

        verify(clockWaiter, timeout(10_000).times(3)).waitFor(any());

        Catalog catalog0 = manager.catalog(manager.latestCatalogVersion());

        assertNotNull(catalog0);

        HybridTimestamp activationSkew = CatalogUtils.clusterWideEnsuredActivationTsSafeForRoReads(
                catalog0,
                () -> partitionIdleSafeTimePropagationPeriod.get(), clockService.maxClockSkewMillis());

        clock.update(activationSkew);

        assertTrue(waitForCondition(createTableFuture1::isDone, 2_000));
        assertTrue(waitForCondition(commandFuture::isDone, 2_000));

        assertSame(manager.schema(catalogVerAfterTableCreate), manager.activeSchema(clock.nowLong()));
    }

    @Test
    public void catalogServiceManagesUpdateLogLifecycle() {
        UpdateLog updateLogMock = mock(UpdateLog.class);
        ComponentContext startComponentContext = new ComponentContext();
        ComponentContext stopComponentContext = new ComponentContext();

        when(updateLogMock.startAsync(startComponentContext)).thenReturn(nullCompletedFuture());
        when(updateLogMock.stopAsync(stopComponentContext)).thenReturn(nullCompletedFuture());
        when(updateLogMock.append(any())).thenReturn(CompletableFuture.completedFuture(true));

        CatalogManagerImpl manager = new CatalogManagerImpl(updateLogMock, clockService);

        assertThat(manager.startAsync(startComponentContext), willCompleteSuccessfully());

        verify(updateLogMock).startAsync(startComponentContext);

        assertThat(manager.stopAsync(stopComponentContext), willCompleteSuccessfully());

        verify(updateLogMock).stopAsync(stopComponentContext);
    }

    @Test
    public void userFutureCompletesAfterClusterWideActivationHappens() {
        delayDuration.set(TimeUnit.DAYS.toMillis(365));

        reset(clockWaiter);
        HybridTimestamp startTs = clock.now();

        CompletableFuture<?> commandFuture = manager.execute(TestCommand.ok());

        assertFalse(commandFuture.isDone());

        ArgumentCaptor<HybridTimestamp> tsCaptor = ArgumentCaptor.forClass(HybridTimestamp.class);

        verify(clockWaiter, timeout(10_000)).waitFor(tsCaptor.capture());
        HybridTimestamp userWaitTs = tsCaptor.getValue();
        assertThat(
                userWaitTs.getPhysical() - startTs.getPhysical(),
                greaterThanOrEqualTo(delayDuration.get() + clockService.maxClockSkewMillis())
        );
    }

    // TODO: remove after IGNITE-20378 is implemented.
    @Test
    public void userFutureCompletesAfterClusterWideActivationWithAdditionalIdleSafeTimePeriodHappens() {
        delayDuration.set(TimeUnit.DAYS.toMillis(365));
        partitionIdleSafeTimePropagationPeriod.set(TimeUnit.DAYS.toDays(365));

        reset(clockWaiter);

        HybridTimestamp startTs = clock.now();

        CompletableFuture<?> commandFuture = manager.execute(TestCommand.ok());

        assertFalse(commandFuture.isDone());

        ArgumentCaptor<HybridTimestamp> tsCaptor = ArgumentCaptor.forClass(HybridTimestamp.class);

        verify(clockWaiter, timeout(10_000)).waitFor(tsCaptor.capture());
        HybridTimestamp userWaitTs = tsCaptor.getValue();
        assertThat(
                userWaitTs.getPhysical() - startTs.getPhysical(),
                greaterThanOrEqualTo(
                        delayDuration.get() + clockService.maxClockSkewMillis()
                                + partitionIdleSafeTimePropagationPeriod.get() + clockService.maxClockSkewMillis()
                )
        );
    }

    @Test
    void testLatestCatalogVersion() {
        assertEquals(1, manager.latestCatalogVersion());

        assertThat(manager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertEquals(2, manager.latestCatalogVersion());

        assertThat(manager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertEquals(3, manager.latestCatalogVersion());
    }

    @Test
    void bulkCommandEitherAppliedAtomicallyOrDoesntAppliedAtAll() {
        List<CatalogCommand> bulkUpdate = List.of(
                TestCommand.ok(),
                TestCommand.ok(),
                TestCommand.fail()
        );

        Catalog catalog = manager.catalog(manager.latestCatalogVersion());
        assertNotNull(catalog);
        int initial = catalog.objectIdGenState();

        assertThat(manager.execute(bulkUpdate), willThrowFast(TestCommandFailure.class));

        // now let's truncate problematic table and retry
        assertThat(manager.execute(bulkUpdate.subList(0, bulkUpdate.size() - 1)), willCompleteSuccessfully());

        Catalog updatedCatalog = manager.catalog(manager.latestCatalogVersion());
        assertNotNull(updatedCatalog);
        assertEquals(2 + initial, updatedCatalog.objectIdGenState());
    }

    @Test
    void bulkUpdateIncrementsVersionByOne() {
        int versionBefore = manager.latestCatalogVersion();

        assertThat(
                manager.execute(List.of(TestCommand.ok(), TestCommand.ok())),
                willCompleteSuccessfully()
        );

        int versionAfter = manager.latestCatalogVersion();

        assertThat(versionAfter - versionBefore, is(1));
    }

    @Test
    void bulkUpdateDoesntIncrementVersionInCaseOfError() {
        int versionBefore = manager.latestCatalogVersion();

        assertThat(
                manager.execute(List.of(TestCommand.ok(), TestCommand.fail())),
                willThrow(TestCommandFailure.class)
        );

        int versionAfter = manager.latestCatalogVersion();

        assertThat(versionAfter, is(versionBefore));
    }

    @Test
    public void testCatalogCompaction() throws Exception {
        assertThat(manager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(manager.execute(TestCommand.ok()), willCompleteSuccessfully());

        long timestamp = clock.nowLong();
        Catalog catalog = manager.catalog(manager.activeCatalogVersion(clock.nowLong()));

        // Add more updates
        assertThat(manager.execute(TestCommand.ok()), willCompleteSuccessfully());
        assertThat(manager.execute(TestCommand.ok()), willCompleteSuccessfully());

        assertThat(manager.compactCatalog(timestamp), willBe(Boolean.TRUE));
        assertTrue(waitForCondition(() -> catalog.version() == manager.earliestCatalogVersion(), 3_000));

        assertNull(manager.catalog(0));
        assertNull(manager.catalog(catalog.version() - 1));
        assertNotNull(manager.catalog(catalog.version()));

        assertThrows(IllegalStateException.class, () -> manager.activeCatalogVersion(0));
        assertThrows(IllegalStateException.class, () -> manager.activeCatalogVersion(catalog.time() - 1));
        assertSame(catalog.version(), manager.activeCatalogVersion(catalog.time()));
        assertSame(catalog.version(), manager.activeCatalogVersion(timestamp));

        assertThat(manager.compactCatalog(timestamp), willBe(false));
        assertEquals(catalog.version(), manager.earliestCatalogVersion());
    }

    @Test
    public void testEmptyCatalogCompaction() {
        assertEquals(1, manager.latestCatalogVersion());

        long timestamp = clock.nowLong();

        assertThat(manager.compactCatalog(timestamp), willBe(false));

        assertEquals(0, manager.earliestCatalogVersion());
        assertEquals(1, manager.latestCatalogVersion());

        assertNotNull(manager.catalog(1));

        assertEquals(0, manager.activeCatalogVersion(0));
        assertEquals(1, manager.activeCatalogVersion(timestamp));
    }

    /** Test command that does nothing but increments object id counter of a catalog. */
    private static class TestCommand implements CatalogCommand {

        private final boolean successful;

        private TestCommand(boolean successful) {
            this.successful = successful;
        }

        static TestCommand ok() {
            return new TestCommand(true);
        }

        static TestCommand fail() {
            return new TestCommand(false);
        }

        @Override
        public List<UpdateEntry> get(Catalog catalog) {
            if (!successful) {
                throw new TestCommandFailure();
            }
            return List.of(new ObjectIdGenUpdateEntry(1));
        }
    }

    private static class TestCommandFailure extends RuntimeException {
        private static final long serialVersionUID = -6123535862914825943L;
    }
}
