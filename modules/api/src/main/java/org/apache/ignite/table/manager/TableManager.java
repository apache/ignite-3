package org.apache.ignite.table.manager;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableInit;
import org.apache.ignite.table.Table;

/**
 * Interface for manage tables.
 */
public interface TableManager {

    /**
     * Creates a cluster table.
     * The table changes if already exists.
     *
     * @param name Table name.
     * @param tableInitChange Table changer.
     * @return Table.
     */
    Table createTable(String name, Consumer<TableInit> tableInitChange);

    /**
     * Gets a list of all started tables.
     *
     * @return List of tables.
     */
    List<Table> tables();

    /**
     * Gets a table by name, if it was created before.
     *
     * @param name Name of the table.
     * @return Tables with corresponding name or {@code null} if table isn't created.
     */
    Table table(String name);
}
