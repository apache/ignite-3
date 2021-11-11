package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Ignite table.
 */
public interface IgniteTable extends TranslatableTable, Wrapper {
    /**
     * Get table descriptor.
     *
     * @return Table descriptor.
     */
    TableDescriptor descriptor();
    
    /**
     * {@inheritDoc}
     */
    @Override
    default RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return getRowType(typeFactory, null);
    }
    
    /**
     * Returns new type according {@code usedColumns} param.
     *
     * @param typeFactory     Factory.
     * @param requiredColumns Used columns enumeration.
     */
    RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns);
    
    /**
     * {@inheritDoc}
     */
    @Override
    default TableScan toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return toRel(context.getCluster(), relOptTable);
    }
    
    /**
     * Converts table into relational expression.
     *
     * @param cluster   Custer.
     * @param relOptTbl Table.
     * @return Table relational expression.
     */
    TableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl);
    
    /**
     * Returns table distribution.
     *
     * @return Table distribution.
     */
    IgniteDistribution distribution();
    
    /**
     * {@inheritDoc}
     */
    default Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }
    
    /**
     * {@inheritDoc}
     */
    default boolean isRolledUp(String column) {
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    default boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            @Nullable SqlNode parent,
            @Nullable CalciteConnectionConfig config) {
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    default Statistic getStatistic() {
        return new Statistic() {
            @Override
            public List<RelCollation> getCollations() {
                return List.of();
            }
        };
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    default <C> @Nullable C unwrap(Class<C> cls) {
        if (cls.isInstance(this)) {
            return cls.cast(this);
        }
        return null;
    }
}
