package org.apache.ignite.internal.sql.engine.prepare;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.ignite.internal.util.Pair;

/**
 * Represents a collection of non-overlapping intervals [from, till).
 */
public class QueryPlanChain<T> {
    // Guarded by synchronized,
    private final TreeMap<Integer, VersionItem<T>> map = new TreeMap<>();

    //private final IntSet tableIds;

    // TODO remove from here
    private final CatalogChangesTrackerImpl changesTracker;

    QueryPlanChain(CatalogChangesTrackerImpl changesTracker) {
        // TODO do someting for empty table ids (dummy implementation?)
        //this.tableIds = tableIds;
        this.changesTracker = changesTracker;
    }

    synchronized VersionItem<T> get(int version) {
        Map.Entry<Integer, VersionItem<T>> floor = map.floorEntry(version);

        if (floor != null) {
            if (floor.getKey() == version) {
                return floor.getValue();
            }

            VersionItem<T> interval = floor.getValue();

            if (interval.validTill >= version) {
                return interval;
            }

            if (interval.invalidFrom == null) {
                // open interval
                // try to find close bound
                int from = interval.validTill;

                List<Integer> bounds = changesTracker.findChangesBetween(from + 1, version + 1, interval.tableIds);

                if (bounds.isEmpty()) {
                    // update valid interval
                    // we know that verfied upper bound
                    interval.validTill = version;

                    return interval;
                }

                // close current interval - to simplify next search
                interval.invalidFrom = bounds.get(0);

                // detect table drop if needed also
                if (changesTracker.droppedOn(bounds, interval.tableIds)) {
                    interval.droppedFrom = bounds.get(0);
                }
            }
        }

        return null;
    }

    VersionItem<T> putIfAbsent(int version, Supplier<T> payloadSupplier) {
        return putIfAbsent(version, payloadSupplier, null);
    }

    /**
     * Adds a new element if no overlapping interval exists.
     *
     * @param version version to insert
     * @param payloadSupplier supplier for payload creation
     * @return the inserted VersionItem or previous value
     */
    synchronized VersionItem<T> putIfAbsent(int version, Supplier<T> payloadSupplier, @Nullable IntSet tableIds) {
        // Check existing interval
        Map.Entry<Integer, VersionItem<T>> floor = map.floorEntry(version);

        if (floor != null) {
            if (floor.getKey() == version) {
                return floor.getValue();
            }

            VersionItem<T> interval = floor.getValue();

            if (interval.validTill >= version) {
                return interval;
            }

            if (interval.invalidFrom == null) {
                // open interval
                // try to find close bound
                int from = interval.validTill;

                List<Integer> bounds = changesTracker.findChangesBetween(from + 1, version + 1, interval.tableIds);

                if (bounds.isEmpty()) {
                    // update valid interval
                    // we know that verified upper bound
                    interval.validTill = version;

                    return interval;
                }

                // close current interval
                interval.invalidFrom = bounds.get(0);

                if (changesTracker.droppedOn(bounds, interval.tableIds)) {
                    interval.droppedFrom = bounds.get(0);

                    // TODO No ideas
                    return null;
                }

                // and create new interval
                int start = bounds.get(bounds.size() - 1);

                interval = new VersionItem<>(interval.tableIds, start, payloadSupplier.get());
                interval.validTill = version;

                VersionItem<T> prev = map.put(start, interval);

                assert prev == null : prev;

                return interval;
            } else if (interval.invalidFrom > version) {
                return interval;
            }
        }

        // need to figure out what type of interval it is
        // ...<none> ... target ... ??
        // need only figure out if there any interval
        VersionItem<T> newInterval;

        if (trackableVersion(version)) {
            // First interval - should be open
            // need to fix startVer
            // update valid interval
            // we know that verfied upper bound

            if (tableIds == null) {
                assert floor != null;

                if (floor.getValue().droppedFrom != null) {
                    // TODO Going to plan from scratch and crate new chain with new table ids.
                    return null;
                }

                tableIds = floor.getValue().tableIds;
            }

            Pair<Integer, Integer> minMaxBounds = changesTracker.lookupNearbyChanges(version, tableIds);

            // TODO tableIds can be incorrect?
            newInterval = new VersionItem<>(tableIds, minMaxBounds.getFirst(), payloadSupplier.get(), minMaxBounds.getSecond());
            // can be ignored if tillVer is not null
            newInterval.validTill = version;
            map.put(minMaxBounds.getFirst(), newInterval);
        } else {
            // Create closed interval
            // Interval us closed - no need to track changes.
            newInterval = new VersionItem<>(IntSet.of(), version, payloadSupplier.get(), version + 1);
            map.put(version, newInterval);
        }

        return newInterval;
    }

    // `false` means that the version is `below` visible changes.
    // So the re-planning will be preformed for each catalog version
    // until we can track changes
    private boolean trackableVersion(int ver) {
        @Nullable Integer minVer = changesTracker.minVersion();

        if (minVer == null) {
            return false;
        }

        return ver >= minVer;
    }

    VersionItem<T> tail() {
        return map.lastEntry().getValue();
    }

    /**
     * Inner class representing a versioned interval with payload.
     */
    static class VersionItem<T> {
        // This is used only in tests and should be removed.
        final int validFrom;
        final T payload;

        // points to last known valid version (modifiable)
        Integer validTill;
        // points to first version that is not valid (null or constant value)
        @Nullable Integer invalidFrom;

        @Nullable Integer droppedFrom;

        final IntSet tableIds;

        VersionItem(IntSet tableIds, int validFrom, T payload) {
            this(tableIds, validFrom, payload, null);
        }

        VersionItem(IntSet tableIds, int validTill, T payload, @Nullable Integer invalidFrom) {
            this.tableIds = tableIds;
            this.validFrom = validTill;
            this.validTill = validTill;
            this.payload = payload;
            this.invalidFrom = invalidFrom;
        }

        @Override
        public String toString() {
            return "VersionItem{"
                    + "validFrom=" + validFrom
                    + ", payload=" + payload
                    + ", validTill=" + validTill
                    + ", invalidFrom=" + invalidFrom
                    + ", droppedFrom=" + droppedFrom
                    + ", tableIds=" + tableIds
                    + '}';
        }
    }
}
