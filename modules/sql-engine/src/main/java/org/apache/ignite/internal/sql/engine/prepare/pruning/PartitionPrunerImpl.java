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

package org.apache.ignite.internal.sql.engine.prepare.pruning;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.MappedFragment;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.jetbrains.annotations.Nullable;

/** Applies partition pruning. */
public class PartitionPrunerImpl implements PartitionPruner {

    private static final IgniteLogger LOG = Loggers.forClass(PartitionPrunerImpl.class);

    /** Constructor. */
    public PartitionPrunerImpl() {

    }

    /** {@inheritDoc} */
    @Override
    public List<MappedFragment> apply(
            List<MappedFragment> mappedFragments,
            Object[] dynamicParameters
    ) {
        List<MappedFragment> updatedFragments = new ArrayList<>(mappedFragments.size());
        Long2ObjectMap<List<String>> newNodesByExchangeId = new Long2ObjectArrayMap<>();

        // Partition pruning (PP). For each fragment:
        //
        // 1. Extract PP metadata from eac in the form of [colo_col1=<val>, ..] (see PartitionPruningMetadataExtractor)
        //
        // 2. If PP metadata exists then update fragment's colocation group
        // to retain partition that are necessary to perform an operator (e.g. for a scan operator such
        // partitions only include that ones that can contain data).
        //
        // Iterate over fragments again to update fragments that receive data from fragments updated at step 2.
        // This is accomplished by updating `sourcesByExchangeId`.
        //

        for (MappedFragment mappedFragment : mappedFragments) {
            // Fragment that contains colocated operators has exactly one colocation group.
            // Do not attempt to apply PP to other fragments.
            if (mappedFragment.groups().size() != 1) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            Fragment fragment = mappedFragment.fragment();
            if (fragment.tables().isEmpty()) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            PartitionPruningMetadataExtractor extractor = new PartitionPruningMetadataExtractor();
            PartitionPruningMetadata pruningMetadata = extractor.go(fragment.root());

            if (pruningMetadata.data().isEmpty()) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            // Update fragment by applying PP metadata.
            MappedFragment newFragment = updateColocationGroups(mappedFragment, pruningMetadata, dynamicParameters);

            if (newFragment == null) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            // Collect new nodes for each exchange that was affected by PP.
            if (fragment.root() instanceof IgniteSender) {
                long exchangeId = ((IgniteSender) fragment.root()).exchangeId();
                newNodesByExchangeId.put(exchangeId, newFragment.nodes());
            }

            updatedFragments.add(newFragment);
        }

        // No exchange was updated, return.
        if (newNodesByExchangeId.isEmpty()) {
            return updatedFragments;
        }

        // Update source->exchange mapping for every fragment that receives data from fragments affected by PP.

        boolean updatedExchangers = false;

        for (int j = 0; j < mappedFragments.size(); j++) {
            MappedFragment mappedFragment = mappedFragments.get(j);
            MappedFragment newFragment = updateSourceExchanges(mappedFragment, newNodesByExchangeId);

            if (newFragment != null) {
                updatedFragments.set(j, newFragment);
                updatedExchangers = true;
            }
        }

        assert updatedExchangers : "No source exchange was updated. Mapping is probably broken " + newNodesByExchangeId;

        return updatedFragments;
    }

    @Nullable
    private static MappedFragment updateColocationGroups(
            MappedFragment mappedFragment,
            PartitionPruningMetadata pruningMetadata,
            Object[] dynamicParameters
    ) {

        Fragment fragment = mappedFragment.fragment();
        Long2ObjectMap<ColocationGroup> newColocationGroups = new Long2ObjectArrayMap<>();

        for (Entry<IgniteTable> entry : fragment.tables().long2ObjectEntrySet()) {
            long sourceId = entry.getLongKey();
            IgniteTable table = entry.getValue();

            PartitionPruningColumns pruningColumns = pruningMetadata.get(sourceId);
            if (pruningColumns == null) {
                continue;
            }

            ColocationGroup colocationGroup = mappedFragment.groupsBySourceId().get(sourceId);
            assert colocationGroup != null : "No colocation group#" + sourceId;
            // Ensure that partition pruning is applied only a fragment with colocated operators.
            assert colocationGroup.nodeNames().containsAll(mappedFragment.nodes())
                    && colocationGroup.nodeNames().size() == mappedFragment.nodes().size()
                    : "ColocationGroup/Fragment nodes do not match: " + colocationGroup.nodeNames() + " " + mappedFragment.nodes();


            PartitionPruningPredicate pruningPredicate = new PartitionPruningPredicate(table, pruningColumns, dynamicParameters);
            ColocationGroup newColocationGroup = pruningPredicate.prunePartitions(colocationGroup);

            newColocationGroups.put(sourceId, newColocationGroup);
        }

        if (newColocationGroups.isEmpty()) {
            return null;
        }

        MappedFragment newFragment = mappedFragment.replaceColocationGroups(newColocationGroups);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Applied partition pruning to fragment#{}\ncurrent groups: {}\nnew groups: {}",
                    fragment.fragmentId(),
                    mappedFragment.groups(),
                    newFragment.groups()
            );
        }

        return newFragment;
    }

    @Nullable
    private static MappedFragment updateSourceExchanges(MappedFragment mappedFragment, Long2ObjectMap<List<String>> newNodesByExchangeId) {
        Fragment fragment = mappedFragment.fragment();
        MappedFragment[] result = {mappedFragment};

        IgniteRelShuttle updater = new IgniteRelShuttle() {
            @Override
            public IgniteRel visit(IgniteReceiver rel) {
                long exchangeId = rel.exchangeId();
                List<String> newSourcesByExchange = newNodesByExchangeId.get(exchangeId);
                if (newSourcesByExchange == null) {
                    return super.visit(rel);
                }

                MappedFragment current = result[0];
                result[0] = current.replaceExchangeSources(exchangeId, newSourcesByExchange);

                return super.visit(rel);
            }
        };

        fragment.root().accept(updater);

        if (result[0] != mappedFragment) {
            return result[0];
        } else {
            return null;
        }
    }
}
