package org.apache.ignite.distributed;

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
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.impl.HeapUnboundedLockManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager.LockState;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

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

    protected TableImpl testTable;

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
            protected TxManagerImpl newTxManager(ReplicaService replicaSvc, HybridClock clock, TransactionIdGenerator generator,
                    ClusterNode node, PlacementDriver placementDriver) {
                return new TxManagerImpl(
                        replicaSvc,
                        new HeapLockManager(
                                DeadlockPreventionPolicy.NO_OP,
                                HeapLockManager.SLOTS,
                                CACHE_SIZE,
                                new HeapUnboundedLockManager()),
                        clock,
                        generator,
                        node::id,
                        placementDriver
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
}
