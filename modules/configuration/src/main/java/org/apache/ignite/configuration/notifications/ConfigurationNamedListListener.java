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

package org.apache.ignite.configuration.notifications;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.NotNull;

/**
 * Configuration property change listener for named list configurations.
 *
 * @param <VIEW> VIEW type configuration.
 */
public interface ConfigurationNamedListListener<VIEW> extends ConfigurationListener<VIEW> {
    /**
     * Called when new named list element is created.
     *
     * @param ctx Notification context.
     * @return Future that signifies end of listener execution.
     */
    @NotNull CompletableFuture<?> onCreate(ConfigurationNotificationEvent<VIEW> ctx);

    /**
     * Called when named list element is deleted.
     *
     * @param ctx Notification context.
     * @return Future that signifies end of listener execution.
     */
    @NotNull CompletableFuture<?> onDelete(ConfigurationNotificationEvent<VIEW> ctx);
}
