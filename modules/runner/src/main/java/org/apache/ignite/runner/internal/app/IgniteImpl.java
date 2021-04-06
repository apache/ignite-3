package org.apache.ignite.runner.internal.app;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.table.distributed.internal.DistributedTableManager;

public class IgniteImpl implements Ignite {
    private final ConfigurationManager configurationMgr;

    private final DistributedTableManager distributedTblMgr;

    IgniteImpl(
        ConfigurationManager configurationMgr,
        DistributedTableManager distributedTblMgr)
    {
        this.configurationMgr = configurationMgr;
        this.distributedTblMgr = distributedTblMgr;
    }

    @Override public void close() throws Exception {
        // TODO sanpwc: Implement.
    }
}
