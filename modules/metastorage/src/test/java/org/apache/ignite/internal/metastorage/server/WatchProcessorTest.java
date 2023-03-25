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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for {@link WatchProcessor}.
 */
public class WatchProcessorTest {
    private final WatchProcessor watchProcessor = new WatchProcessor("test", WatchProcessorTest::oldEntry);

    private final OnRevisionAppliedCallback revisionCallback = mock(OnRevisionAppliedCallback.class);

    @BeforeEach
    void setUp() {
        when(revisionCallback.onRevisionApplied(any(), any())).thenReturn(completedFuture(null));

        watchProcessor.setRevisionCallback(revisionCallback);
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

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, "foo".getBytes(UTF_8))));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, "bar".getBytes(UTF_8))));

        var entry1 = new EntryImpl("foo".getBytes(UTF_8), null, 1, 0);
        var entry2 = new EntryImpl("bar".getBytes(UTF_8), null, 1, 0);

        watchProcessor.notifyWatches(List.of(entry1, entry2));

        verify(listener1, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry1), entry1)));
        verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));

        verify(listener1, never()).onRevisionUpdated(anyLong());
        verify(listener2, never()).onRevisionUpdated(anyLong());

        // Revision callback should be called for every listener update.
        verify(revisionCallback, timeout(1_000).times(2)).onRevisionApplied(any(), any());
    }

    /**
     * Tests a scenario when updates for different listeners come sequentially.
     */
    @Test
    void testSequentialEventNotification() {
        WatchListener listener1 = mockListener();
        WatchListener listener2 = mockListener();

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, "foo".getBytes(UTF_8))));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, "bar".getBytes(UTF_8))));

        var entry1 = new EntryImpl("foo".getBytes(UTF_8), null, 1, 0);
        var entry2 = new EntryImpl("bar".getBytes(UTF_8), null, 2, 0);

        watchProcessor.notifyWatches(List.of(entry1));

        verify(listener1, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry1), entry1)));
        verify(listener2, timeout(1_000)).onRevisionUpdated(1);

        watchProcessor.notifyWatches(List.of(entry2));

        verify(listener1, timeout(1_000)).onRevisionUpdated(2);
        verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));

        // Revision callback should be called for every listener update.
        verify(revisionCallback, timeout(1_000).times(4)).onRevisionApplied(any(), any());
    }

    /**
     * Tests a scenario that, when a watch throws an exception, it gets disabled and other watches continue working.
     */
    @Test
    void testWatchFailure() {
        WatchListener listener1 = mockListener();

        WatchListener listener2 = mock(WatchListener.class);

        when(listener2.id()).thenReturn("error");
        when(listener2.onUpdate(any())).thenThrow(new IllegalStateException());

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, "foo".getBytes(UTF_8))));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, "bar".getBytes(UTF_8))));

        var entry1 = new EntryImpl("foo".getBytes(UTF_8), null, 1, 0);
        var entry2 = new EntryImpl("bar".getBytes(UTF_8), null, 1, 0);

        watchProcessor.notifyWatches(List.of(entry1, entry2));

        verify(listener1, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry1), entry1)));
        verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));
        verify(listener2, timeout(1_000)).onError(any(IllegalStateException.class));

        var entry3 = new EntryImpl("bar".getBytes(UTF_8), null, 2, 0);

        watchProcessor.notifyWatches(List.of(entry3));

        clearInvocations(listener1, listener2, revisionCallback);

        verify(listener1, timeout(1_000)).onRevisionUpdated(2);

        verify(listener2, never()).onUpdate(any());
        verify(listener2, never()).onError(any());
        verify(listener2, never()).onRevisionUpdated(anyLong());

        verify(revisionCallback, timeout(1_000)).onRevisionApplied(any(), any());
    }

    /**
     * Tests watch notification independence (two watches can be processed simultaneously) and linearizability
     * (one watch can process one event at a time).
     */
    @Test
    void testNotificationParallelism() {
        WatchListener listener1 = mockListener();

        WatchListener listener2 = mock(WatchListener.class);

        when(listener2.id()).thenReturn("blocking");

        var blockingFuture = new CompletableFuture<Void>();

        when(listener2.onUpdate(any()))
                // Block the first call, the second call should work as usual.
                .thenReturn(blockingFuture)
                .thenReturn(completedFuture(null));

        watchProcessor.addWatch(new Watch(0, listener1, key -> Arrays.equals(key, "foo".getBytes(UTF_8))));
        watchProcessor.addWatch(new Watch(0, listener2, key -> Arrays.equals(key, "bar".getBytes(UTF_8))));

        var entry1 = new EntryImpl("foo".getBytes(UTF_8), null, 1, 0);
        var entry2 = new EntryImpl("bar".getBytes(UTF_8), null, 1, 0);

        watchProcessor.notifyWatches(List.of(entry1, entry2));

        verify(listener1, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry1), entry1)));
        verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));

        var entry3 = new EntryImpl("foo".getBytes(UTF_8), null, 2, 0);
        var entry4 = new EntryImpl("bar".getBytes(UTF_8), null, 2, 0);

        watchProcessor.notifyWatches(List.of(entry3, entry4));

        verify(listener1, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry3), entry3)));
        verify(listener2, never()).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry4), entry4)));

        blockingFuture.complete(null);

        InOrder inOrder = inOrder(listener2);

        inOrder.verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry2), entry2)));
        inOrder.verify(listener2, timeout(1_000)).onUpdate(new WatchEvent(new EntryEvent(oldEntry(entry4), entry4)));
    }

    private static WatchListener mockListener() {
        var listener = mock(WatchListener.class);

        String id = UUID.randomUUID().toString();

        when(listener.id()).thenReturn(id);

        when(listener.onUpdate(any())).thenReturn(completedFuture(null));
        when(listener.onRevisionUpdated(anyLong())).thenReturn(completedFuture(null));

        return listener;
    }

    private static Entry oldEntry(byte[] key, long revision) {
        return new EntryImpl(key, null, revision, 0);
    }

    private static Entry oldEntry(Entry entry) {
        return new EntryImpl(entry.key(), null, entry.revision() - 1, 0);
    }
}
