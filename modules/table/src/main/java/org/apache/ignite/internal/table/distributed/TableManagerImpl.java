package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.metastorage.internal.MetaStorageManager;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.schema.internal.SchemaManager;
import org.apache.ignite.table.manager.TableManager;

public class TableManagerImpl implements TableManager {
    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Network cluster. */
    private final ClusterService clusterNetSvc;

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    public TableManagerImpl(
        ConfigurationManager configurationMgr,
        ClusterService clusterNetSvc,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaManager
    ) {
        this.configurationMgr = configurationMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaManager = schemaManager;
    }
}
