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

package org.apache.ignite.internal.tx.impl;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * A composite id for resources, which consists of context id and local id. Context id can be shared by multiple resources.
 */
public class FullyQualifiedResourceId implements Comparable<FullyQualifiedResourceId>, Cloneable, Serializable {

    private static final UUID LOWEST_UUID = new UUID(Long.MIN_VALUE, Long.MIN_VALUE);

    private static final UUID HIGHEST_UUID = new UUID(Long.MAX_VALUE, Long.MAX_VALUE);

    private static int compareNullable(@Nullable UUID contextId1, @Nullable UUID contextId2) {
        if (contextId1 == null && contextId2 == null) {
            return 0;
        } else if (contextId1 == null) {
            return -1;
        } else if (contextId2 == null) {
            return 1;
        } else {
            return contextId1.compareTo(contextId2);
        }
    }

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Global identifier. */
    private final @Nullable UUID contextId;

    /** Local identifier. */
    private final UUID resourceId;

    /**
     * Constructs {@code FullyQualifiedResourceId} from a global and local identifiers.
     *
     * @param contextId Context id.
     * @param resourceId Resource id.
     */
    public FullyQualifiedResourceId(@Nullable UUID contextId, UUID resourceId) {
        assert resourceId != null;

        this.contextId = contextId;
        this.resourceId = resourceId;
    }

    public static FullyQualifiedResourceId lower(UUID contextId) {
        return new FullyQualifiedResourceId(contextId, LOWEST_UUID);
    }

    public static FullyQualifiedResourceId upper(UUID contextId) {
        return new FullyQualifiedResourceId(contextId, HIGHEST_UUID);
    }

    /**
     * Gets global ID portion of this {@code FullyQualifiedResourceId}.
     *
     * @return Global ID portion of this {@code FullyQualifiedResourceId}.
     */
    public @Nullable UUID contextId() {
        return contextId;
    }

    /**
     * Gets local ID portion of this {@code FullyQualifiedResourceId}.
     *
     * @return Local ID portion of this {@code FullyQualifiedResourceId}.
     */
    public UUID resourceId() {
        return resourceId;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(FullyQualifiedResourceId o) {
        if (o == this) {
            return 0;
        }

        int res = compareNullable(contextId, o.contextId());

        if (res == 0) {
            res = resourceId.compareTo(o.resourceId());
        }

        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FullyQualifiedResourceId that = (FullyQualifiedResourceId) o;
        return Objects.equals(contextId, that.contextId) && Objects.equals(resourceId, that.resourceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextId, resourceId);
    }

    /** {@inheritDoc} */
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        if (contextId == null) {
            return String.valueOf(resourceId);
        }
        return String.valueOf(contextId) + '/' + resourceId;
    }

}
