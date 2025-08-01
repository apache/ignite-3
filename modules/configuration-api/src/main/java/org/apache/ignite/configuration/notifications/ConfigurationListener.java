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

package org.apache.ignite.configuration.notifications;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Configuration property change listener.
 *
 * @param <VIEWT> VIEW type configuration.
 */
@FunctionalInterface
public interface ConfigurationListener<VIEWT> {
    /**
     * Called on property value update.
     *
     * @param ctx Notification context.
     * @return Future that signifies the end of the listener execution.
     */
    CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<VIEWT> ctx);

    /** Creates an adapter for a given callback. */
    static <T> ConfigurationListener<T> fromConsumer(Consumer<ConfigurationNotificationEvent<T>> callback) {
        return ctx -> {
            try {
                callback.accept(ctx);
            } catch (Throwable e) {
                return failedFuture(e);
            }

            return nullCompletedFuture();
        };
    }
}
