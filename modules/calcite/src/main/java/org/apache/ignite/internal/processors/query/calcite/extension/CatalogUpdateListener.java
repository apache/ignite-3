package org.apache.ignite.internal.processors.query.calcite.extension;

import org.apache.ignite.internal.processors.query.calcite.extension.SqlExtension.ExternalCatalog;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;

/**
 * Listener used to notify {@link SchemaHolder} about any changes in the external catalogs.
 */
public interface CatalogUpdateListener {
    /**
     * Notify the {@link SchemaHolder} that provided catalog has been updated.
     *
     * @param catalog Catalog to notify the {@link SchemaHolder} about.
     */
    void onCatalogUpdated(ExternalCatalog catalog);
}
