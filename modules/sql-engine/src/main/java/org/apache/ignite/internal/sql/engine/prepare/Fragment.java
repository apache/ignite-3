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

import static org.apache.ignite.internal.sql.engine.externalize.RelJsonWriter.toJson;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.schema.IgniteSystemView;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Fragment of distributed query.
 */
public class Fragment {
    private final long id;

    private final IgniteRel root;

    /** Serialized root representation. */
    @IgniteToStringExclude
    private final String rootSer;

    private final List<IgniteReceiver> remotes;
    private final Long2ObjectMap<IgniteTable> tables;
    private final List<IgniteSystemView> systemViews;

    private final boolean correlated;

    /**
     * Constructor.
     *
     * @param id An identifier of this fragment.
     * @param correlated Whether some correlated variables should be set prior to fragment execution.
     * @param root Root node of the fragment.
     * @param remotes Remote sources of the fragment.
     * @param tables A list of tables containing by this fragment.
     * @param systemViews A list of system views containing by this fragment.
     */
    public Fragment(long id, boolean correlated, IgniteRel root, List<IgniteReceiver> remotes,
            Long2ObjectMap<IgniteTable> tables, List<IgniteSystemView> systemViews) {
        this.id = id;
        this.root = root;
        this.remotes = List.copyOf(remotes);
        this.tables = new Long2ObjectArrayMap<>(tables);
        this.systemViews = List.copyOf(systemViews);
        this.rootSer = toJson(root);
        this.correlated = correlated;
    }

    /**
     * Get fragment ID.
     */
    public long fragmentId() {
        return id;
    }

    /**
     * Get root node.
     */
    public IgniteRel root() {
        return root;
    }

    /**
     * Lazy serialized root representation.
     *
     * @return Serialized form.
     */
    public String serialized() {
        return rootSer;
    }

    /**
     * Returns {@code true} if this fragment expecting some correlated variables being set from
     * outside (e.g. parent fragment).
     *
     * @return {@code true} if correlated variables should be set prior to start the execution of this fragment.
     */
    public boolean correlated() {
        return correlated;
    }

    /**
     * Get fragment remote sources.
     */
    public List<IgniteReceiver> remotes() {
        return remotes;
    }

    public Long2ObjectMap<IgniteTable> tables() {
        return tables;
    }

    public List<IgniteSystemView> systemViews() {
        return systemViews;
    }

    public boolean rootFragment() {
        return !(root instanceof IgniteSender);
    }

    /** Returns id of target fragment for non-root fragments, return {@code null} otherwise. */
    public @Nullable Long targetFragmentId() {
        if (root instanceof IgniteSender) {
            return ((IgniteSender) root).targetFragmentId();
        }

        return null;
    }

    public boolean single() {
        return rootFragment() || (root instanceof IgniteSender
                && ((IgniteSender) root).sourceDistribution().satisfies(IgniteDistributions.single()));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Fragment.class, this, "root", RelOptUtil.toString(root));
    }
}
