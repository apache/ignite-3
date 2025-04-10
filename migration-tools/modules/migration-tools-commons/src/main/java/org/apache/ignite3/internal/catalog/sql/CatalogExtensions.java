/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite3.internal.catalog.sql;

import org.apache.ignite3.catalog.definitions.TableDefinition;

/** Utility methods based on the Ignite 3 Catalog package. */
public class CatalogExtensions {
    private CatalogExtensions() {
        // Intentionally left blank.
    }

    public static String sqlFromTableDefinition(TableDefinition def) {
        return new CreateFromDefinitionImpl(null).from(def).toString();
    }
}
