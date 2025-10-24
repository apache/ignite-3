package org.apache.ignite.internal.configuration;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Closure interface to be used by the configuration changer. An instance of this closure is passed into the constructor and invoked every
 * time when there's an update from any of the storages.
 */
public interface ConfigurationUpdateListener {
    /**
     * Invoked every time when the configuration is updated.
     *
     * @param oldRoot Old roots values. All these roots always belong to a single storage.
     * @param newRoot New values for the same roots as in {@code oldRoot}.
     * @param storageRevision Configuration revision of the storage.
     * @param notificationNumber Configuration listener notification number.
     * @return Future that must signify when processing is completed. Exceptional completion is not expected.
     */
    CompletableFuture<Void> onConfigurationUpdated(
            @Nullable SuperRoot oldRoot, SuperRoot newRoot, long storageRevision, long notificationNumber
    );
}
