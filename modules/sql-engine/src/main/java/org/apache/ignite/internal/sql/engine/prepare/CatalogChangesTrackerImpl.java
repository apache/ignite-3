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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;
import org.apache.ignite.internal.util.Pair;

/**
 * Catalog changes tracker.
 * TODO Split into several classes.
 */
public class CatalogChangesTrackerImpl implements CatalogTableDependencyEventListener {
    private final NavigableMap<Integer, Set<Integer>> changes = new ConcurrentSkipListMap<>();

    private final ConcurrentMap<Integer, Set<Integer>> drops = new ConcurrentHashMap<>();

    @Nullable Integer minVersion() {
        Entry<Integer, Set<Integer>> entry = changes.firstEntry();

        return entry == null ? null : entry.getKey();
    }

    @Override
    public void onTableDependencyChanged(int catalogVersion, int tableId) {
        changes.computeIfAbsent(catalogVersion, v -> ConcurrentHashMap.newKeySet()).add(tableId);
    }

    @Override
    public void onTableDrop(int catalogVersion, int tableId) {
        drops.computeIfAbsent(catalogVersion, v -> ConcurrentHashMap.newKeySet()).add(tableId);
        onTableDependencyChanged(catalogVersion, tableId);
    }

    List<Integer> findChangesBetween(int lowInclusive, int highExclusive, Set<Integer> tableIds) {
        List<Integer> result = new ArrayList<>();

        for (Map.Entry<Integer, Set<Integer>> e : changes.subMap(lowInclusive, highExclusive).entrySet()) {
            for (int tableId : tableIds) {
                if (e.getValue().contains(tableId)) {
                    result.add(e.getKey());
                }
            }
        }

        return result;
    }

    //  chain: 1, 3, 8, 7
    //  input: 4
    // result: 3, 8
    Pair<Integer, Integer> lookupNearbyChanges(int version, Set<Integer> tableIds) {
        NavigableMap<Integer, Set<Integer>> left = changes.headMap(version, true);
        NavigableMap<Integer, Set<Integer>> right = changes.tailMap(version, false);

        Integer low = lookupChange(left.descendingMap().entrySet(), tableIds);

        if (low == null) {
            // we cannot be here if this is an empty map, so the first entry must exist
            low = changes.firstKey();
        }

        Integer high = lookupChange(right.entrySet(), tableIds);

        return new Pair<>(low, high);
    }

    boolean droppedOn(List<Integer> versions, Set<Integer> tableIds) {
        for (int version : versions) {
            Set<Integer> drop = drops.get(version);

            if (drop == null) {
                continue;
            }

            for (int tableId : tableIds) {
                if (drop.contains(tableId)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Nullable private static Integer lookupChange(Set<Map.Entry<Integer, Set<Integer>>> entrySet, Set<Integer> ids) {
        for (Map.Entry<Integer, Set<Integer>> e : entrySet) {
            for (int tableId : ids) {
                if (e.getValue().contains(tableId)) {
                    return e.getKey();
                }
            }
        }

        return null;
    }
}
