package org.apache.ignite.internal.causality;

import static java.util.concurrent.CompletableFuture.allOf;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/** Implementation for tests. */
public class TestRevisionListenerRegistry implements RevisionListenerRegistry {
    private final List<RevisionListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void listen(RevisionListener listener) {
        listeners.add(listener);
    }

    /** Updates revision. */
    public CompletableFuture<Void> updateRevision(long revision) {
        CompletableFuture<?>[] futures = listeners.stream()
                .map(m -> m.onUpdate(revision))
                .toArray(CompletableFuture[]::new);

        return allOf(futures);
    }

    /** Deletes revisions. */
    public void deleteRevisions(long revisionUpperBoundInclusive) {
        listeners.forEach(listener -> listener.onDelete(revisionUpperBoundInclusive));
    }
}
