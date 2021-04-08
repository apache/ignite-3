package org.apache.ignite.runner.internal.app;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.lang.LogWrapper;
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
