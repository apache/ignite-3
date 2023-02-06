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

package org.apache.ignite.internal.sql.engine.metadata;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.io.Serializable;
import java.util.List;

/**
 * FragmentDescription.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class FragmentDescription implements Serializable {
    private static final long serialVersionUID = 0L;

    private final long fragmentId;
    private final boolean prefetch;
    private final FragmentMapping mapping;
    private final ColocationGroup target;
    private final Long2ObjectMap<List<String>> remoteSources;

    /**
     * Constructor.
     *
     * @param fragmentId An identifier of the fragment.
     * @param prefetch A flag denoting whether this fragment may be executed in advance.
     * @param mapping A mapping of the described fragment.
     * @param target A target group this fragment should stream data to.
     * @param remoteSources A mapping of sources this fragment should receive data from.
     */
    public FragmentDescription(
            long fragmentId,
            boolean prefetch,
            FragmentMapping mapping,
            ColocationGroup target,
            Long2ObjectMap<List<String>> remoteSources
    ) {
        this.fragmentId = fragmentId;
        this.prefetch = prefetch;
        this.mapping = mapping;
        this.target = target;
        this.remoteSources = remoteSources;
    }

    /** Returns {@code true} if it's safe to execute this fragment in advance. */
    public boolean prefetch() {
        return prefetch;
    }

    /**
     * Get fragment id.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * Get node ids.
     */
    public List<String> nodeNames() {
        return mapping.nodeNames();
    }

    /**
     * Get target.
     */
    public ColocationGroup target() {
        return target;
    }

    /**
     * Get remotes.
     */
    public Long2ObjectMap<List<String>> remotes() {
        return remoteSources;
    }

    /**
     * Get mapping.
     */
    public FragmentMapping mapping() {
        return mapping;
    }
}
