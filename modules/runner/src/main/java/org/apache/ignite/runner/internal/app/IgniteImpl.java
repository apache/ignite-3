package org.apache.ignite.runner.internal.app;

import java.util.function.Consumer;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.distributed.configuration.DistributedTableConfiguration;
import org.apache.ignite.table.distributed.configuration.TableInit;
import org.apache.ignite.table.manager.TableManager;

/**
 * Ignite internal implementation.
 */
public class IgniteImpl implements Ignite {
    /** Logger. */
    LogWrapper log = new LogWrapper(IgniteImpl.class);

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

    /**
     * @param configurationMgr Configuration manager.
     * @param TblMgr Table manager.
     */
    IgniteImpl(ConfigurationManager configurationMgr, TableManager TblMgr) {
        this.configurationMgr = configurationMgr;
        this.distributedTblMgr = TblMgr;
    }


    /** {@inheritDoc} */
    @Override public Table createTable(String name, Consumer<TableInit> tableInitChange) {
        configurationMgr.configurationRegistry()
            .getConfiguration(DistributedTableConfiguration.KEY).tables().change(change ->
            change.create(name, tableInitChange));

//        this.createTable("tbl1", change -> {
//            change.initBackups(2);
//            change.initName("tbl1");
//            change.initPartitions(1_000);
//        });

        //TODO: Get it honestly using some future.
        Table tbl = null;

        while (tbl == null) {
            try {
                Thread.sleep(50);

                tbl = distributedTblMgr.table(name);
            }
            catch (InterruptedException e) {
                log.error("Waiting of creation of table was interrupted.", e);
            }
        }

        return tbl;
    }

    /** {@inheritDoc} */
    @Override public TableManager tableManager() {
        return distributedTblMgr;
    }

    /** {@inheritDoc} */
    @Override public ConfigurationRegistry configuration() {
        return configurationMgr.configurationRegistry();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // TODO sanpwc: Implement.
    }
}
