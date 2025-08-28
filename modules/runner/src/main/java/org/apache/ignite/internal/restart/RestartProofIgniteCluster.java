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

package org.apache.ignite.internal.restart;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.IgniteCluster;
import org.jetbrains.annotations.Nullable;

/**
 * Reference to {@link IgniteCluster} under a swappable {@link Ignite} instance. When a restart happens, this switches to the new Ignite
 * instance.
 *
 * <p>API operations on this are linearized with respect to node restarts. Normally (except for situations when timeouts trigger), user
 * operations will not interact with detached objects.
 */
public class RestartProofIgniteCluster implements IgniteCluster, Wrapper {
    private final IgniteAttachmentLock attachmentLock;

    RestartProofIgniteCluster(IgniteAttachmentLock attachmentLock) {
        this.attachmentLock = attachmentLock;
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return attachmentLock.attached(ignite -> Wrappers.unwrap(ignite.cluster(), classToUnwrap));
    }

    @Override
    public UUID id() {
        return attachmentLock.attached(ignite -> ignite.cluster().id());
    }

    @Override
    public Collection<ClusterNode> nodes() {
        return attachmentLock.attached(ignite -> ignite.cluster().nodes());
    }

    @Override
    public CompletableFuture<Collection<ClusterNode>> nodesAsync() {
        return attachmentLock.attached(ignite -> ignite.cluster().nodesAsync());
    }

    @Override
    public @Nullable ClusterNode localNode() {
        return attachmentLock.attached(ignite -> ignite.cluster().localNode());
    }
}
