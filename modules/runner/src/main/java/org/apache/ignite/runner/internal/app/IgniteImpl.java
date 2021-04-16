package org.apache.ignite.runner.internal.app;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.table.manager.TableManager;

/**
 * Ignite internal implementation.
 */
public class IgniteImpl implements Ignite {

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteImpl.class);

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

    /**
     * @param TblMgr Table manager.
     */
    IgniteImpl(TableManager TblMgr) {
        this.distributedTblMgr = TblMgr;
    }

    /** {@inheritDoc} */
    @Override public TableManager tableManager() {
        return distributedTblMgr;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // TODO IGNITE-14581 Implement IgniteImpl close method.
    }
}
