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
    /** {@inheritDoc} */
    @Override
    public List<MappedFragment> apply(
            List<MappedFragment> mappedFragments,
            Object[] dynamicParameters,
            PartitionPruningMetadata metadata
    ) {
        if (metadata.data().isEmpty()) {
            return mappedFragments;
        }

        List<MappedFragment> updatedFragments = new ArrayList<>(mappedFragments.size());
        Long2ObjectMap<List<String>> newNodesByExchangeId = new Long2ObjectArrayMap<>();

        // Partition pruning (PP). For each fragment:
        //
        // If PP metadata exists then update fragment's colocation group
        // to retain partition that are necessary to perform an operator (e.g. for a scan operator such
        // partitions only include that ones that can contain data).

        for (MappedFragment mappedFragment : mappedFragments) {
            Fragment fragment = mappedFragment.fragment();
            if (fragment.tables().isEmpty()) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            PartitionPruningMetadata fragmentPruningMetadata = metadata.subset(fragment.tables());

            if (fragmentPruningMetadata.data().isEmpty()) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            // Do not update colocation groups, in case when predicates include correlated variables,
            // because partitions for such case can be removed only at runtime.
            boolean containCorrelatedVariables = fragmentPruningMetadata.data().values()
                    .stream()
                    .anyMatch(PartitionPruningColumns::containCorrelatedVariables);

            if (containCorrelatedVariables) {
                updatedFragments.add(mappedFragment.withPartitionPruningMetadata(fragmentPruningMetadata));
                continue;
            }

            // Update fragment by applying PP metadata.
            MappedFragment newFragment = updateColocationGroups(mappedFragment, fragmentPruningMetadata, dynamicParameters);

            if (newFragment == null) {
                updatedFragments.add(mappedFragment);
                continue;
            }

            /*
            Collect new nodes for each exchange that was affected by PP.

            Say we have fragment#0 that receives data from fragment#1, these fragments are connected via exchange.
            Both fragments store node names separately (see exchangeSourceNodes for fragment#0 and executionNodes for fragment#1),
            to preserve connectivity after partition pruning, we need to update both fragments.
            The example below demonstrates mapping prior to pruning:

            SELECT * FROM t2_n1n2 WHERE id = 42
            ---
            Fragment#0 root
              executionNodes: [N1]
              remoteFragments: [1]
              exchangeSourceNodes: {1=[N1, N2]}
              tree:
                IgniteReceiver(sourceFragment=1, exchange=1, distribution=single)

            Fragment#1
              targetNodes: [N1]
              executionNodes: [N1, N2]
              tables: [T2_N1N2]
              partitions: {N1=[0:1], N2=[1:1]}
              tree:
                IgniteSender(targetFragment=0, exchange=1, distribution=single)
                  IgniteTableScan(name=PUBLIC.T2_N1N2, source=2, partitions=2, distribution=affinity[table: T2_N1N2, columns: [ID]])

            After pruning is applied, and say it removes partition#0 (N1=[0:1]). Then fragment#0 should have exchangeSourceNodes={1=[N2]}.
            and fragment#1 should have executionNodes=[N1] and partitions={N2=[1:1]}.
             */

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

        for (int i = 0; i < mappedFragments.size(); i++) {
            MappedFragment mappedFragment = mappedFragments.get(i);
            MappedFragment newFragment = updateSourceExchanges(mappedFragment, newNodesByExchangeId);

            if (newFragment != null) {
                updatedFragments.set(i, newFragment);
                updatedExchangers = true;
            }
        }

        assert updatedExchangers : "No source exchange was updated. Mapping is probably broken " + newNodesByExchangeId;

        return updatedFragments;
    }

    private static @Nullable MappedFragment updateColocationGroups(
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

            ColocationGroup newColocationGroup = PartitionPruningPredicate.prunePartitions(
                    sourceId,
                    table, pruningColumns, dynamicParameters,
                    colocationGroup
            );

            newColocationGroups.put(sourceId, newColocationGroup);
        }

        if (newColocationGroups.isEmpty()) {
            return null;
        }

        return mappedFragment.replaceColocationGroups(newColocationGroups);
    }

    private static @Nullable MappedFragment updateSourceExchanges(
            MappedFragment mappedFragment,
            Long2ObjectMap<List<String>> newNodesByExchangeId
    ) {
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
