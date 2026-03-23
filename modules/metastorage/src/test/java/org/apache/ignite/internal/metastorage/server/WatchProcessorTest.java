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

package org.apache.ignite.internal.metastorage.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/**
 * Tests for {@link WatchProcessor}.
 */
public class WatchProcessorTest extends BaseIgniteAbstractTest {
    private static final HybridTimestamp TIMESTAMP = HybridTimestamp.MAX_VALUE;

    private static final byte[] FOO_BYTES = "foo".getBytes(UTF_8);
    private static final byte[] BAR_BYTES = "bar".getBytes(UTF_8);

    private final FailureManager failureManager = mock(FailureManager.class);

    private final WatchProcessor watchProcessor = new WatchProcessor(
            "test",
            WatchProcessorTest::oldEntry,
            failureManager
    );

    private final WatchEventHandlingCallback watchEventHandlingCallback = mock(WatchEventHandlingCallback.class);

    @BeforeEach
    void setUp() {
        watchProcessor.setWatchEventHandlingCallback(watchEventHandlingCallback);
    }

    @AfterEach
    void tearDown() {
        watchProcessor.close();
    }

    /**
     * Tests a scenario when updates for different listeners come in a single batch.
     */
    @Test
    void testGroupEventNotification() {
        WatchListener listener1 = mockListener();
        WatchListener listener2 = mockListener();

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, FOO_BYTES)));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, BAR_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);
        var entry2 = new EntryImpl(BAR_BYTES, null, 1, TIMESTAMP);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(1, List.of(entry1, entry2), HybridTimestamp.MAX_VALUE);

        assertThat(notificationFuture, willCompleteSuccessfully());

        var entryEvent1 = new EntryEvent(oldEntry(entry1), entry1);
        var entryEvent2 = new EntryEvent(oldEntry(entry2), entry2);

        verify(listener1).onUpdate(new WatchEvent(entryEvent1));
        verify(listener2).onUpdate(new WatchEvent(entryEvent2));

        verify(watchEventHandlingCallback).onRevisionApplied(1L);
    }

    /**
     * Tests a scenario when updates for different listeners come sequentially.
     */
    @Test
    void testSequentialEventNotification() {
        WatchListener listener1 = mockListener();
        WatchListener listener2 = mockListener();

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, FOO_BYTES)));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, BAR_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);
        var entry2 = new EntryImpl(BAR_BYTES, null, 2, TIMESTAMP);

        HybridTimestamp ts = new HybridTimestamp(1, 2);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(1, List.of(entry1), ts);

        assertThat(notificationFuture, willCompleteSuccessfully());

        var event = new WatchEvent(new EntryEvent(oldEntry(entry1), entry1));

        verify(listener1).onUpdate(event);

        verify(watchEventHandlingCallback).onRevisionApplied(1L);

        ts = new HybridTimestamp(2, 3);

        notificationFuture = watchProcessor.notifyWatches(2, List.of(entry2), ts);

        assertThat(notificationFuture, willCompleteSuccessfully());

        event = new WatchEvent(new EntryEvent(oldEntry(entry2), entry2));

        verify(listener2).onUpdate(event);

        verify(watchEventHandlingCallback).onRevisionApplied(2L);
    }

    /**
     * Tests a scenario that, when a watch throws an exception, watch processing finishes with an error.
     */
    @Test
    void testWatchFailure() {
        WatchListener listener1 = mockListener();

        WatchListener listener2 = mock(WatchListener.class);

        when(listener2.onUpdate(any())).thenThrow(new IllegalStateException());

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, FOO_BYTES)));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, BAR_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);
        var entry2 = new EntryImpl(BAR_BYTES, null, 1, TIMESTAMP);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(1, List.of(entry1, entry2), HybridTimestamp.MAX_VALUE);

        assertThat(notificationFuture, willThrow(IllegalStateException.class));

        verify(listener1).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry1), entry1)));
        verify(listener2).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));

        verify(watchEventHandlingCallback, never()).onRevisionApplied(anyLong());
    }

    /**
     * Tests watch notification independence (two watches can be processed simultaneously) and linearizability
     * (one watch can process one event at a time).
     */
    @Test
    void testNotificationParallelism() {
        WatchListener listener1 = mockListener();

        WatchListener listener2 = mock(WatchListener.class);

        var blockingFuture = new CompletableFuture<Void>();

        when(listener2.onUpdate(any()))
                // Block the first call, the second call should work as usual.
                .thenReturn(blockingFuture)
                .thenReturn(nullCompletedFuture());

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, FOO_BYTES)));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, BAR_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);
        var entry2 = new EntryImpl(BAR_BYTES, null, 1, TIMESTAMP);

        watchProcessor.notifyWatches(1, List.of(entry1, entry2), HybridTimestamp.MAX_VALUE);

        verify(listener1, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry1), entry1)));
        verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));

        var entry3 = new EntryImpl(FOO_BYTES, null, 2, TIMESTAMP);
        var entry4 = new EntryImpl(BAR_BYTES, null, 2, TIMESTAMP);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(2, List.of(entry3, entry4), HybridTimestamp.MAX_VALUE);

        verify(listener1, never()).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry3), entry3)));
        verify(listener2, never()).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry4), entry4)));

        blockingFuture.complete(null);

        assertThat(notificationFuture, willCompleteSuccessfully());

        verify(listener1).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry3), entry3)));

        InOrder inOrder = inOrder(listener2);

        inOrder.verify(listener2).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));
        inOrder.verify(listener2).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry4), entry4)));
    }

    @Test
    void testEmptyEvents() {
        WatchListener listener = mockListener();

        watchProcessor.addWatch(new Watch(0, listener, key -> Arrays.equals(key, FOO_BYTES)));

        var entry = new EntryImpl(BAR_BYTES, null, 1, TIMESTAMP);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(1, List.of(entry), HybridTimestamp.MAX_VALUE);

        assertThat(notificationFuture, willCompleteSuccessfully());

        verify(listener, never()).onUpdate(any());
    }

    @Test
    void exceptionTriggersFailureManager() {
        WatchListener listener = mockListener();
        when(listener.onUpdate(any())).thenReturn(failedFuture(new RuntimeException("Oops")));

        watchProcessor.addWatch(new Watch(0, listener, key -> Arrays.equals(key, FOO_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(1, List.of(entry1), HybridTimestamp.MAX_VALUE);

        assertThat(notificationFuture, willThrow(RuntimeException.class));

        ArgumentCaptor<FailureContext> contextCaptor = ArgumentCaptor.forClass(FailureContext.class);
        verify(failureManager).process(contextCaptor.capture());

        assertThat(contextCaptor.getValue().error(), is(wrappedRuntimeException("Oops")));
    }

    private static Matcher<Throwable> wrappedRuntimeException(String message) {
        return new CustomMatcher<>("a RuntimeException with '" + message + "' message") {
            @Override
            public boolean matches(Object actual) {
                if (actual instanceof CompletionException) {
                    Throwable cause = ((CompletionException) actual).getCause();
                    if (cause instanceof RuntimeException) {
                        return Objects.equals(cause.getMessage(), message);
                    }
                }
                return false;
            }
        };
    }

    @Test
    void nodeStoppingExceptionDoesNotTriggerFailureManager() {
        WatchListener listener = mockListener();
        when(listener.onUpdate(any())).thenReturn(failedFuture(new CompletionException(new NodeStoppingException())));

        watchProcessor.addWatch(new Watch(0, listener, key -> Arrays.equals(key, FOO_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);

        CompletableFuture<Void> notificationFuture = watchProcessor.notifyWatches(1, List.of(entry1), HybridTimestamp.MAX_VALUE);

        assertThat(notificationFuture, willThrow(NodeStoppingException.class));

        verify(failureManager, never()).process(any());
    }

    @Test
    void eventNotificationUpdatesMetastoreSafeTimeAfterNotifyingWatchListeners() {
        var listenerFuture = new CompletableFuture<Void>();
        WatchListener listener = mock(WatchListener.class);
        when(listener.onUpdate(any())).thenReturn(listenerFuture);

        watchProcessor.addWatch(new Watch(0, listener, key -> Arrays.equals(key, FOO_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);
        var ts = new HybridTimestamp(1, 2);

        watchProcessor.notifyWatches(1, List.of(entry1), ts);

        verify(watchEventHandlingCallback, timeout(250).times(0)).onSafeTimeAdvanced(any());

        listenerFuture.complete(null);

        verify(watchEventHandlingCallback, timeout(SECONDS.toMillis(10))).onSafeTimeAdvanced(ts);
    }

    @Test
    void metastoreSafeTimeGetsAdvancedAfterPreviousNotificationChainMembesAreFinished() {
        var listenerFuture = new CompletableFuture<Void>();
        WatchListener listener = mock(WatchListener.class);
        when(listener.onUpdate(any())).thenReturn(listenerFuture);

        watchProcessor.addWatch(new Watch(0, listener, key -> Arrays.equals(key, FOO_BYTES)));

        var entry1 = new EntryImpl(FOO_BYTES, null, 1, TIMESTAMP);
        HybridTimestamp entryTs = new HybridTimestamp(1, 2);
        HybridTimestamp laterTs = entryTs.addPhysicalTime(10);

        watchProcessor.notifyWatches(1, List.of(entry1), entryTs);
        watchProcessor.advanceSafeTime(() -> {}, laterTs);

        verify(watchEventHandlingCallback, timeout(250).times(0)).onSafeTimeAdvanced(laterTs);

        listenerFuture.complete(null);

        verify(watchEventHandlingCallback, timeout(SECONDS.toMillis(10))).onSafeTimeAdvanced(laterTs);
    }

    private static WatchListener mockListener() {
        var listener = mock(WatchListener.class);

        when(listener.onUpdate(any())).thenReturn(nullCompletedFuture());

        return listener;
    }

    private static Entry oldEntry(byte[] key, long revision) {
        return new EntryImpl(key, null, revision, TIMESTAMP);
    }

    private static Entry oldEntry(Entry entry) {
        return new EntryImpl(entry.key(), null, entry.revision() - 1, TIMESTAMP);
    }
}
