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

package org.apache.ignite.internal.network.serialization;

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Contains information about a class layer, both from local and remote classes. Any of the parts (local/remote) might be absent.
 */
public class MergedLayer {
    @Nullable
    private final ClassDescriptor localLayer;
    @Nullable
    private final ClassDescriptor remoteLayer;

    /**
     * Creates an instance with local layer only.
     *
     * @param localLayer    the local layer
     * @return an instance with local layer only
     */
    public static MergedLayer localOnly(ClassDescriptor localLayer) {
        return new MergedLayer(localLayer, null);
    }

    /**
     * Creates an instance with remote layer only.
     *
     * @param remoteLayer the remote layer
     * @return an instance with remote layer only
     */
    public static MergedLayer remoteOnly(ClassDescriptor remoteLayer) {
        return new MergedLayer(null, remoteLayer);
    }

    /**
     * Constructs a new instance.
     */
    public MergedLayer(@Nullable ClassDescriptor localLayer, @Nullable ClassDescriptor remoteLayer) {
        assert localLayer != null || remoteLayer != null : "Both descriptors are null";
        assert localLayer == null || remoteLayer == null || Objects.equals(localLayer.className(), remoteLayer.className())
                : "Descriptors of different classes: " + localLayer.className() + " and " + remoteLayer.className();

        this.localLayer = localLayer;
        this.remoteLayer = remoteLayer;
    }

    /**
     * Returns {@code true} if local layer is present.
     *
     * @return {@code true} if local layer is present
     */
    public boolean hasLocal() {
        return localLayer != null;
    }

    /**
     * Returns local layer.
     *
     * @return local layer
     */
    public ClassDescriptor local() {
        return Objects.requireNonNull(localLayer, "localLayer is null");
    }

    /**
     * Returns {@code true} if remote layer is present.
     *
     * @return {@code true} if remote layer is present
     */
    public boolean hasRemote() {
        return remoteLayer != null;
    }

    /**
     * Returns remote layer.
     *
     * @return remote layer
     */
    public ClassDescriptor remote() {
        return Objects.requireNonNull(remoteLayer, "remoteLayer is null");
    }
}
