package org.apache.ignite.internal.sql.engine.prepare;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlanChain.VersionItem;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class CacheManagerTest {
    private final CacheFactory cacheFactory = CaffeineCacheFactory.INSTANCE;
    static CompletableFuture<TestPayload> planFut() {
        return new CompletableFuture<>();
    }

    static CompletableFuture<TestPayload> throwingPlanFut() {
        throw new IllegalStateException("Should not be called");
    }

    @Test
    void testCacheManager() {
        CatalogChangesTrackerImpl changesTracker = new CatalogChangesTrackerImpl();
        QueryPlanCacheManager<TestPayload> cacheManager = new QueryPlanCacheManager<>(changesTracker, cacheFactory);
        CacheKeyBase key = new CacheKeyBase("PUBLIC", "SELECT 1", new ColumnType[0]);

        changesTracker.onTableDependencyChanged(1, 1);

        CompletableFuture<TestPayload> planFut1 =
                cacheManager.get(key.withCatalogVersion(1), k -> CompletableFuture.completedFuture(new TestPayload("1", IntSet.of(1))));

        // since future is already completed - the plan must be in the stable cache with an open interval
        CompletableFuture<TestPayload> planFut2 = cacheManager.get(key.withCatalogVersion(2), k -> CompletableFuture.completedFuture(new TestPayload("1", IntSet.of(1))));

        assertThat(planFut1, Matchers.is(planFut2));

        changesTracker.onTableDependencyChanged(3, 1);

        CompletableFuture<TestPayload> planFut3 = cacheManager.get(key.withCatalogVersion(2), k -> planFut());

        assertThat(planFut3, Matchers.is(planFut2));

        CompletableFuture<TestPayload> planFut4 = cacheManager.get(key.withCatalogVersion(3), k -> planFut());

        assertThat(planFut4, Matchers.not(Matchers.is(planFut2)));
    }

    @Test
    void testCatalogChangedDuringPlanning() {
        CatalogChangesTrackerImpl changesTracker = new CatalogChangesTrackerImpl();
        QueryPlanCacheManager<TestPayload> cacheManager = new QueryPlanCacheManager<>(changesTracker, cacheFactory);
        CacheKeyBase key = new CacheKeyBase("PUBLIC", "SELECT 1", new ColumnType[0]);

        CompletableFuture<TestPayload> planFut1 = cacheManager.get(key.withCatalogVersion(1), k -> planFut());

        changesTracker.onTableDependencyChanged(2, 1);
        changesTracker.onTableDependencyChanged(2, 2);

        CompletableFuture<TestPayload> planFut2 = cacheManager.get(key.withCatalogVersion(2), k -> planFut());

        assertThat(planFut1, Matchers.not(Matchers.is(planFut2)));
    }

    // T1: planFut1: SELECT * FROM A
    // T2: DROP TABLE B
    // T3: planFut2: SELECT * FROM A
    // planFut1 and planFut2 must be different because catalog was changed during planning
    // Until planning is not finished for planFut1 -> if new request with DIFFRERENT catalog version comes -> new planning
    //                        must be started, until we know tables involved
    @Test
    void testParallelPlanningSameKeyDiffCatalog() {
        CatalogChangesTrackerImpl changesTracker = new CatalogChangesTrackerImpl();
        QueryPlanCacheManager<TestPayload> cacheManager = new QueryPlanCacheManager<>(changesTracker, cacheFactory);
        CacheKeyBase key = new CacheKeyBase("PUBLIC", "SELECT 1", new ColumnType[0]);

        changesTracker.onTableDependencyChanged(1, 1);

        CompletableFuture<TestPayload> planFut1 = cacheManager.get(key.withCatalogVersion(1), k -> planFut());
        CompletableFuture<TestPayload> planFut2 = cacheManager.get(key.withCatalogVersion(2), k -> planFut());

        assertThat(planFut1, Matchers.not(Matchers.is(planFut2)));

        planFut1.complete(new TestPayload("1", IntSet.of(1)));

        // This is an exception - need to complete future in temp cache, otherwise the next call will get value from this cache.
        // This is happened because we avoiding duplicate planning if temp cache prepares future for particular catalog version.
        planFut2.complete(new TestPayload("2", IntSet.of(1)));

        TestPayload content = await(cacheManager.get(key.withCatalogVersion(2), k -> throwingPlanFut()));
        assertThat(content.payload, Matchers.is("1"));

        // Should not affect stable cache content
        planFut2.complete(new TestPayload("2", IntSet.of(2)));

        TestPayload contentNew = await(cacheManager.get(key.withCatalogVersion(2), k -> throwingPlanFut()));
        assertThat(contentNew, Matchers.is(content));
    }

    // similar to the above but target table changed during planning
    @Test
    void testParallelPlanningSameKeyDiffCatalog2() {
        CatalogChangesTrackerImpl changesTracker = new CatalogChangesTrackerImpl();
        QueryPlanCacheManager<TestPayload> cacheManager = new QueryPlanCacheManager<>(changesTracker, cacheFactory);
        CacheKeyBase key = new CacheKeyBase("PUBLIC", "SELECT 1", new ColumnType[0]);

        changesTracker.onTableDependencyChanged(1, 1);
        CompletableFuture<TestPayload> planFut1 = cacheManager.get(key.withCatalogVersion(1), k -> planFut());

        changesTracker.onTableDependencyChanged(2, 1);
        CompletableFuture<TestPayload> planFut2 = cacheManager.get(key.withCatalogVersion(2), k -> planFut());

        assertThat(planFut1, Matchers.not(Matchers.is(planFut2)));

        planFut1.complete(new TestPayload("1", IntSet.of(1)));

        CompletableFuture<TestPayload> planFut3 = cacheManager.get(key.withCatalogVersion(2), k -> throwingPlanFut());
        // cache with catalog version must be taken into account - avoid second planning
        assertThat(planFut3, Matchers.is(planFut2));

        planFut2.complete(new TestPayload("2", IntSet.of(1)));

        // Next request for open interval must return correct value
        TestPayload content = await(cacheManager.get(key.withCatalogVersion(2), k -> throwingPlanFut()));
        assertThat(content.payload, Matchers.is("2"));

        // Should not affect stable cache content
        planFut3.complete(new TestPayload("2", IntSet.of(2)));

        TestPayload contentNew = await(cacheManager.get(key.withCatalogVersion(2), k -> throwingPlanFut()));
        assertThat(contentNew, Matchers.is(content));
    }

    @Test
    void testSortedMap() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();

        tracker.onTableDependencyChanged(2, 1);
        tracker.onTableDependencyChanged(3, 1);
        tracker.onTableDependencyChanged(5, 1);
        tracker.onTableDependencyChanged(7, 1);

        assertThat(tracker.findChangesBetween(3, 6, Set.of(1)),
                Matchers.is(List.of(3, 5)));
    }

    // This is case when catalog changes are empty (node was restarted)
    // After restart the plan valid for current catalog version only,
    // because we don't know is it historucal query or not (cannot create open interval)
    @Test
    void singleOpenIntervalNoChangesTracked() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();

        QueryPlanChain<String> versions = new QueryPlanChain<>(tracker);

        VersionItem<String> interval1 = versions.putIfAbsent(2, () -> "a", IntSet.of(1, 2));

        assertThat(interval1.validTill, Matchers.is(2));
        assertThat(interval1.invalidFrom, Matchers.is(3));

        VersionItem<String> interval2 = versions.putIfAbsent(4, () -> "b");
        assertThat(interval2.validTill, Matchers.is(4));
        assertThat(interval2.invalidFrom, Matchers.is(5));

        tracker.onTableDependencyChanged(5, 23);

        VersionItem<String> interval3 = versions.putIfAbsent(3, () -> "c");

        assertThat(interval3.validTill, Matchers.is(3));
        assertThat(interval3.invalidFrom, Matchers.is(4));

        VersionItem<String> interval4 = versions.putIfAbsent(6, () -> "d");
        assertThat(interval4.validTill, Matchers.is(6));
        assertThat(interval4.invalidFrom, Matchers.is(Matchers.nullValue()));
        assertThat(interval4.validFrom, Matchers.is(5)); // tracked version
    }

    @Test
    void firstClosedInterval() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();
        tracker.onTableDependencyChanged(1, 1);
        tracker.onTableDependencyChanged(5, 1);

        QueryPlanChain<String> map = new QueryPlanChain<>(tracker);

        VersionItem<String> interval = map.putIfAbsent(4, () -> "a", IntSet.of(1, 2));

        assertThat(interval.validFrom, Matchers.is(1));
        assertThat(interval.invalidFrom, Matchers.is(5));
    }

    @Test
    void historical() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();
        tracker.onTableDependencyChanged(1, 1);

        QueryPlanChain<String> map = new QueryPlanChain<>(tracker);
        VersionItem<String> interval1 = map.putIfAbsent(3, () -> "a", IntSet.of(1, 2));
        assertThat(interval1.validFrom, Matchers.is(1));
        assertThat(interval1.invalidFrom, Matchers.nullValue());

        tracker.onTableDependencyChanged(4, 1);

        tracker.onTableDependencyChanged(7, 1);
        VersionItem<String> lastInterval = map.putIfAbsent(8, () -> "b");

        assertThat(interval1.invalidFrom, Matchers.is(4));

        assertThat(lastInterval.payload, Matchers.is("b"));
        assertThat(lastInterval.validFrom, Matchers.is(7));
        assertThat(lastInterval.invalidFrom, Matchers.nullValue());

        // We have gap 4 -> 6
        // <1 -> 3>, <7 -> ...>
        // historical query must be re-planned for each cat ver separately, because
        // NO IT MUST NOT
        // we have track history - we can figure out andcover whole gap with single plan
        {
            VersionItem<String> historical = map.putIfAbsent(6, () -> "h3");
            assertThat(historical.payload, Matchers.is("h3"));
            assertThat(historical.validFrom, Matchers.is(4));
            assertThat(historical.invalidFrom, Matchers.is(7));
        }

        {
            VersionItem<String> historical = map.putIfAbsent(4, throwingSupplier());
            assertThat(historical.payload, Matchers.is("h3"));
            assertThat(historical.validFrom, Matchers.is(4));
            assertThat(historical.invalidFrom, Matchers.is(7));
        }

        {
            VersionItem<String> historical = map.putIfAbsent(5, throwingSupplier());
            assertThat(historical.payload, Matchers.is("h3"));
            assertThat(historical.validFrom, Matchers.is(4));
            assertThat(historical.invalidFrom, Matchers.is(7));
        }

        // Ensure that last interval is still open
        assertThat(lastInterval.payload, Matchers.is("b"));
        assertThat(lastInterval.validFrom, Matchers.is(7));
        assertThat(lastInterval.invalidFrom, Matchers.nullValue());
    }

    @Test
    void trackerBoundsSearch() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();

        tracker.onTableDependencyChanged(2, 15);
        tracker.onTableDependencyChanged(5, 12);
        tracker.onTableDependencyChanged(7, 15);

        { // untracked table ids
            Pair<Integer, Integer> bounds = tracker.lookupNearbyChanges(4, Set.of(42));

            assertThat(bounds.getFirst(), Matchers.is(2));
            assertThat(bounds.getSecond(), Matchers.is(Matchers.nullValue()));
        }

        {
            Pair<Integer, Integer> bounds = tracker.lookupNearbyChanges(4, Set.of(15));

            assertThat(bounds.getFirst(), Matchers.is(2));
            assertThat(bounds.getSecond(), Matchers.is(7));
        }

        {
            Pair<Integer, Integer> bounds = tracker.lookupNearbyChanges(4, Set.of(12));

            assertThat(bounds.getFirst(), Matchers.is(2));
            assertThat(bounds.getSecond(), Matchers.is(5));
        }

        {
            Pair<Integer, Integer> bounds = tracker.lookupNearbyChanges(5, Set.of(12));

            assertThat(bounds.getFirst(), Matchers.is(5));
            assertThat(bounds.getSecond(), Matchers.is(Matchers.nullValue()));
        }
    }

    @Test
    void singleOpenInterval() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();
        tracker.onTableDependencyChanged(1, 1);

        QueryPlanChain<String> map = new QueryPlanChain<>(tracker);

        VersionItem<String> interval = map.putIfAbsent(2, () -> "a", IntSet.of(1, 2));

        assertThat(interval.validFrom, Matchers.is(1));
        assertThat(interval.validTill, Matchers.is(2));

        VersionItem<String> interval3 = map.putIfAbsent(3, throwingSupplier());
        VersionItem<String> interval100 = map.putIfAbsent(100, throwingSupplier());

        assertThat(interval3.payload, Matchers.is("a"));
        assertThat(interval100.payload, Matchers.is("a"));
    }

    @Test
    void autoCloseInterval() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();

        QueryPlanChain<String> map = new QueryPlanChain<>(tracker);

        tracker.onTableDependencyChanged(2, 1);

        VersionItem<String> interval1 = map.putIfAbsent(2, () -> "a", IntSet.of(1, 2));

        // the order must be enforced - catalog changes listener must be updated BEFORE put plan into cache
        tracker.onTableDependencyChanged(3, 2);

        tracker.onTableDependencyChanged(4, 1);

        VersionItem<String> interval2 = map.putIfAbsent(5, () -> "b");

        assertThat(interval1.payload, Matchers.is("a"));
        assertThat(interval1.tableIds, Matchers.is(Set.of(1, 2)));
        assertThat(interval1.validFrom, Matchers.is(2));
        assertThat(interval1.invalidFrom, Matchers.is(3));

        assertThat(interval2.payload, Matchers.is("b"));
        assertThat(interval2.tableIds, Matchers.is(Set.of(1, 2)));
        assertThat(interval2.validFrom, Matchers.is(4));
        assertThat(interval2.validTill, Matchers.is(5));
    }

    @Test
    void dropTable() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();

        QueryPlanCacheManager<TestPayload> cacheManager = new QueryPlanCacheManager<>(tracker, cacheFactory);

        // CREATE TABLE A...
        tracker.onTableDependencyChanged(1, 10);

        CacheKeyBase key = new CacheKeyBase("PUBLIC", "SELECT * FROM A", new ColumnType[0]);

        cacheManager.get(key.withCatalogVersion(1),
                k -> CompletableFuture.completedFuture(new TestPayload("PLAN 10", IntSet.of(10))));

        // DROP TABLE A...
        tracker.onTableDrop(2, 10);

        // CREATE TABLE A... is not tracked
        TestPayload result = await(cacheManager.get(key.withCatalogVersion(3),
                k -> CompletableFuture.completedFuture(new TestPayload("PLAN 11", IntSet.of(11)))));

        assertThat(cacheManager.getChain(key).tail().tableIds, Matchers.is(IntSet.of(11)));
        assertThat(result.payload, Matchers.is("PLAN 11"));
    }

    @Test
    void alterDropTable() {
        CatalogChangesTrackerImpl tracker = new CatalogChangesTrackerImpl();

        QueryPlanCacheManager<TestPayload> cacheManager = new QueryPlanCacheManager<>(tracker, cacheFactory);

        // CREATE TABLE A...
        tracker.onTableDependencyChanged(1, 10);

        CacheKeyBase key = new CacheKeyBase("PUBLIC", "SELECT * FROM A", new ColumnType[0]);

        cacheManager.get(key.withCatalogVersion(1),
                k -> CompletableFuture.completedFuture(new TestPayload("PLAN 10", IntSet.of(10))));

        // ALTER TABLE A...
        tracker.onTableDependencyChanged(2, 10);

        // DROP TABLE A...
        tracker.onTableDrop(3, 10);

        // CREATE TABLE A... (new table with same name but different id)
        TestPayload result = await(cacheManager.get(key.withCatalogVersion(4),
                k -> CompletableFuture.completedFuture(new TestPayload("PLAN 11", IntSet.of(11)))));

        assertThat(cacheManager.getChain(key).tail().tableIds, Matchers.is(IntSet.of(11)));
        assertThat(result.payload, Matchers.is("PLAN 11"));
    }


    <T> Supplier<T> throwingSupplier() {
        return () -> {
            throw new IllegalStateException("Should not be called");
        };
    }

    static class TestPayload implements SourcesAware {
        final Object payload;
        final IntSet sources;

        public TestPayload(Object payload, IntSet sources) {
            this.payload = payload;
            this.sources = sources;
        }

        @Override
        public IntSet sources() {
            return sources;
        }
    }
}
