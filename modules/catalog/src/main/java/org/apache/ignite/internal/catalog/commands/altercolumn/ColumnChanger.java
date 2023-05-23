package org.apache.ignite.internal.catalog.commands.altercolumn;

import java.util.function.Function;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

public interface ColumnChanger extends Function<TableColumnDescriptor, TableColumnDescriptor> {

}
