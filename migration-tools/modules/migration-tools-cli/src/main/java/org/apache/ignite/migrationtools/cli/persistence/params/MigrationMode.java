/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.persistence.params;

/**
 * Allows controlling the error handling policy in case of an irreconcilable mismatch between the GG8 record and the target GG9 Table.
 */
public enum MigrationMode {
    /** Abort the migration. */
    ABORT,
    /** The whole cache record will be ignored (not migrated to the table; lost). */
    SKIP_RECORD,
    /**
     * Any additional columns/fields in the cache record will be ignored (not migrated to the table; lost).
     * The others will be migrated as usual.
     */
    IGNORE_COLUMN,
    /**
     * Any additional columns/fields in the cache record will be serialized to JSON and stored in the `__EXTRA__` column.
     * This is an additional column that the tool adds to the table, it is not a native feature.
     * <p>This mode will allow to leverage the <i>IgniteAdapter.Builder#allowExtraFields</i> feature in the adapter module.</p>
     */
    PACK_EXTRA
}
