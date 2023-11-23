package org.apache.ignite.distributed;

import static org.apache.ignite.internal.replicator.ReplicaManager.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager.LockState;
import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test lock table.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItLockTableTest extends IgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItLockTableTest.class);

    private static int EMP_TABLE_ID = 2;

    private static final int CACHE_SIZE = 10;

    private static final String TABLE_NAME = "test";

    private static SchemaDescriptor TABLE_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("id".toUpperCase(), NativeTypes.INT32, false)},
            new Column[]{
                    new Column("name".toUpperCase(), NativeTypes.STRING, true),
                    new Column("salary".toUpperCase(), NativeTypes.DOUBLE, true)
            }
    );

    protected TableViewInternal testTable;

    protected final TestInfo testInfo;

    //TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    protected static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    protected static GcConfiguration gcConfig;

    private ItTxTestCluster txTestCluster;

    private HybridTimestampTracker timestampTracker = new HybridTimestampTracker();

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItLockTableTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @BeforeEach
    public void before() throws Exception {
        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                gcConfig,
                workDir,
                1,
                1,
                false,
                timestampTracker
        ) {
            @Override
            protected TxManagerImpl newTxManager(
                    ClusterService clusterService,
                    ReplicaService replicaSvc,
                    HybridClock clock,
                    TransactionIdGenerator generator,
                    ClusterNode node,
                    PlacementDriver placementDriver
            ) {
                return new TxManagerImpl(
                        clusterService,
                        replicaSvc,
                        new HeapLockManager(
                                DeadlockPreventionPolicy.NO_OP,
                                HeapLockManager.SLOTS,
                                CACHE_SIZE,
                                new HeapUnboundedLockManager()),
                        clock,
                        generator,
                        placementDriver,
                        () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS
                );
            }
        };
        txTestCluster.prepareCluster();

        testTable = txTestCluster.startTable(TABLE_NAME, EMP_TABLE_ID, TABLE_SCHEMA);

        log.info("Tables have been started");
    }

    @AfterEach
    public void after() throws Exception {
        txTestCluster.shutdownCluster();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20894")
    public void testDeadlockRecovery() {
        RecordView<Tuple> view = testTable.recordView();
        Tuple t1 = tuple(0, "0");
        assertTrue(view.insert(null, t1));

        Tuple t2 = tuple(1, "1");
        assertTrue(view.insert(null, t2));

        InternalTransaction tx1 = (InternalTransaction) txTestCluster.igniteTransactions().begin();
        InternalTransaction tx2 = (InternalTransaction) txTestCluster.igniteTransactions().begin();

        LOG.info("id1={}", tx1.id());
        LOG.info("id2={}", tx2.id());

        assertTrue(tx2.id().compareTo(tx1.id()) > 0);

        Tuple r1_0 = view.get(tx1, keyTuple(0));
        Tuple r2_1 = view.get(tx2, keyTuple(1));

        assertEquals(t1.stringValue("name"), r1_0.stringValue("name"));
        assertEquals(t2.stringValue("name"), r2_1.stringValue("name"));

        view.upsertAsync(tx1, tuple(1, "11"));
        view.upsertAsync(tx2, tuple(0, "00"));

        assertTrue(TestUtils.waitForCondition(() -> {
            int total = 0;
            HeapLockManager lockManager = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
            for (int j = 0; j < lockManager.getSlots().length; j++) {
                LockState slot = lockManager.getSlots()[j];

                total += slot.waitersCount();
            }

            return total == 8;
        }, 10_000), "Some lockers are missing");

        tx1.commit();
    }

    /**
     * Test that a lock table behaves correctly in case of lock cache overflow.
     */
    @Test
    public void testCollision() {
        RecordView<Tuple> view = testTable.recordView();

        int i = 0;
        final int count = 1000;
        List<Transaction> txns = new ArrayList<>();
        while (i++ < count) {
            Transaction tx = txTestCluster.igniteTransactions().begin();
            view.insertAsync(tx, tuple(i, "x-" + i));
            txns.add(tx);
        }

        assertTrue(TestUtils.waitForCondition(() -> {
            int total = 0;
            HeapLockManager lockManager = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
            for (int j = 0; j < lockManager.getSlots().length; j++) {
                LockState slot = lockManager.getSlots()[j];
                total += slot.waitersCount();
            }

            return total == count && lockManager.available() == 0;
        }, 10_000), "Some lockers are missing");

        int empty = 0;
        int coll = 0;

        HeapLockManager lm = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
        for (int j = 0; j < lm.getSlots().length; j++) {
            LockState slot = lm.getSlots()[j];
            int cnt = slot.waitersCount();
            if (cnt == 0) {
                empty++;
            }
            if (cnt > 1) {
                coll += cnt;
            }
        }

        LOG.info("LockTable [emptySlots={} collisions={}]", empty, coll);

        assertTrue(coll > 0);

        List<CompletableFuture<?>> finishFuts = new ArrayList<>();
        for (Transaction txn : txns) {
            finishFuts.add(txn.commitAsync());
        }

        for (CompletableFuture<?> finishFut : finishFuts) {
            finishFut.join();
        }

        assertTrue(TestUtils.waitForCondition(() -> {
            int total = 0;
            HeapLockManager lockManager = (HeapLockManager) txTestCluster.txManagers.get(txTestCluster.localNodeName).lockManager();
            for (int j = 0; j < lockManager.getSlots().length; j++) {
                LockState slot = lockManager.getSlots()[j];
                total += slot.waitersCount();
            }

            return total == 0 && lockManager.available() == CACHE_SIZE;
        }, 10_000), "Illegal lock manager state");
    }

    private static Tuple tuple(int id, String name) {
        return Tuple.create()
                .set("id", id)
                .set("name", name);
    }

    private static Tuple keyTuple(int id) {
        return Tuple.create()
                .set("id", id);
    }
}
