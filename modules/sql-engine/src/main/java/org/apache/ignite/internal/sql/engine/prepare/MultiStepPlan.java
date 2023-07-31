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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.FragmentMapping;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * Regular query or DML.
 */
public class MultiStepPlan implements QueryPlan {

    private final SqlQueryType type;

    protected final ResultSetMetadata meta;

    protected final List<Fragment> fragments;

    /** Constructor. */
    public MultiStepPlan(SqlQueryType type, List<Fragment> fragments, ResultSetMetadata meta) {
        this.type = type;
        this.fragments = fragments;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public SqlQueryType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public QueryPlan copy() {
        return new MultiStepPlan(type, fragments, meta);
    }

    /** Returns {@link FragmentMapping} of the given fragment. */
    public FragmentMapping mapping(Fragment fragment) {
        return getMapping(fragment.fragmentId());
    }

    /** Colocation group for this fragment. */
    public ColocationGroup target(Fragment fragment) {
        if (fragment.rootFragment()) {
            return null;
        }

        IgniteSender sender = (IgniteSender) fragment.root();
        return getMapping(sender.targetFragmentId()).findGroup(sender.exchangeId());
    }

    /** A list for fragment this query plan consists of. */
    public List<Fragment> fragments() {
        return fragments;
    }

    /** Remote nodes. */
    public Long2ObjectOpenHashMap<List<String>> remotes(Fragment fragment) {
        List<IgniteReceiver> remotes = fragment.remotes();

        if (nullOrEmpty(remotes)) {
            return null;
        }

        Long2ObjectOpenHashMap<List<String>> res = new Long2ObjectOpenHashMap<>(capacity(remotes.size()));

        for (IgniteReceiver remote : remotes) {
            res.put(remote.exchangeId(), getMapping(remote.sourceFragmentId()).nodeNames());
        }

        return res;
    }

    /** Creates a copy of this plan replacing its fragment with the given list. */
    public MultiStepPlan replaceFragments(List<Fragment> fragments) {
        return new MultiStepPlan(type, fragments, meta);
    }

    private FragmentMapping getMapping(long fragmentId) {
        return fragments.stream()
                .filter(f -> f.fragmentId() == fragmentId)
                .findAny().orElseThrow(() -> new IllegalStateException("Cannot find fragment with given ID. ["
                        + "fragmentId=" + fragmentId + ", "
                        + "fragments=" + fragments() + "]"))
                .mapping();
    }
}
