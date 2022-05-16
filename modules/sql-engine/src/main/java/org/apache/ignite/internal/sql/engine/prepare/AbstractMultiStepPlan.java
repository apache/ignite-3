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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.ResultSetMetadata;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.FragmentMapping;
import org.apache.ignite.internal.sql.engine.metadata.MappingService;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;

/**
 * AbstractMultiStepPlan.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class AbstractMultiStepPlan implements MultiStepPlan {
    protected final ResultSetMetadata meta;

    protected final QueryTemplate queryTemplate;

    protected ExecutionPlan executionPlan;

    protected AbstractMultiStepPlan(QueryTemplate queryTemplate, ResultSetMetadata meta) {
        this.queryTemplate = queryTemplate;
        this.meta = meta;
    }

    /** {@inheritDoc} */
    @Override
    public List<Fragment> fragments() {
        return Objects.requireNonNull(executionPlan).fragments();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override
    public FragmentMapping mapping(Fragment fragment) {
        return mapping(fragment.fragmentId());
    }

    private FragmentMapping mapping(long fragmentId) {
        return Objects.requireNonNull(executionPlan).fragments().stream()
                .filter(f -> f.fragmentId() == fragmentId)
                .findAny().orElseThrow(() -> new IllegalStateException("Cannot find fragment with given ID. ["
                        + "fragmentId=" + fragmentId + ", "
                        + "fragments=" + fragments() + "]"))
                .mapping();
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup target(Fragment fragment) {
        if (fragment.rootFragment()) {
            return null;
        }

        IgniteSender sender = (IgniteSender) fragment.root();
        return mapping(sender.targetFragmentId()).findGroup(sender.exchangeId());
    }

    /** {@inheritDoc} */
    @Override
    public Long2ObjectOpenHashMap<List<String>> remotes(Fragment fragment) {
        List<IgniteReceiver> remotes = fragment.remotes();

        if (nullOrEmpty(remotes)) {
            return null;
        }

        Long2ObjectOpenHashMap<List<String>> res = new Long2ObjectOpenHashMap<>(capacity(remotes.size()));

        for (IgniteReceiver remote : remotes) {
            res.put(remote.exchangeId(), mapping(remote.sourceFragmentId()).nodeIds());
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public void init(MappingService mappingService, MappingQueryContext ctx) {
        executionPlan = queryTemplate.map(mappingService, ctx);
    }
}
