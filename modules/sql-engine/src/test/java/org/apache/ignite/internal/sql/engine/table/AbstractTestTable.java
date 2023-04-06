package org.apache.ignite.internal.sql.engine.table;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.Nullable;

public class AbstractTestTable implements IgniteTable {
    private final String name;

    private final RelProtoDataType protoType;

    private final Map<String, IgniteIndex> indexes = new HashMap<>();

    private final double rowCnt;

    private final TableDescriptor desc;

    private final UUID id = UUID.randomUUID();

    /** Constructor. */
    public AbstractTestTable(RelDataType type) {
        this(type, 100.0);
    }

    /** Constructor. */
    public AbstractTestTable(RelDataType type, String name) {
        this(name, type, 100.0);
    }

    /** Constructor. */
    public AbstractTestTable(RelDataType type, double rowCnt) {
        this(UUID.randomUUID().toString(), type, rowCnt);
    }

    /** Constructor. */
    public AbstractTestTable(String name, RelDataType type, double rowCnt) {
        protoType = RelDataTypeImpl.proto(type);
        this.rowCnt = rowCnt;
        this.name = name;

        desc = new TestTableDescriptor(this::distribution, type);
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public int version() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalTableScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            List<RelHint> hints,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalTableScan.create(cluster, cluster.traitSet(), hints, relOptTbl, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            String idxName,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalIndexScan.create(cluster, cluster.traitSet(), relOptTbl, idxName, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
        RelDataType rowType = protoType.apply(typeFactory);

        if (bitSet != null) {
            RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
            for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
                b.add(rowType.getFieldList().get(i));
            }
            rowType = b.build();
        }

        return rowType;
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            /** {@inheritDoc} */
            @Override
            public Double getRowCount() {
                return rowCnt;
            }

            /** {@inheritDoc} */
            @Override
            public boolean isKey(ImmutableBitSet cols) {
                return false;
            }

            /** {@inheritDoc} */
            @Override
            public List<ImmutableBitSet> getKeys() {
                throw new AssertionError();
            }

            /** {@inheritDoc} */
            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                throw new AssertionError();
            }

            /** {@inheritDoc} */
            @Override
            public List<RelCollation> getCollations() {
                return Collections.emptyList();
            }

            /** {@inheritDoc} */
            @Override
            public RelDistribution getDistribution() {
                throw new AssertionError();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public Schema.TableType getJdbcTableType() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isRolledUp(String col) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean rolledUpColumnValidInsideAgg(
            String column,
            SqlCall call,
            SqlNode parent,
            CalciteConnectionConfig config
    ) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, IgniteIndex> indexes() {
        return Collections.unmodifiableMap(indexes);
    }

    /** {@inheritDoc} */
    @Override
    public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }

    /**
     * AddIndex.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public AbstractTestTable addIndex(RelCollation collation, String name) {
        indexes.put(name, new IgniteIndex(TestSortedIndex.create(collation, name, this)));

        return this;
    }

    /**
     * AddIndex.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public AbstractTestTable addIndex(String name, int... keys) {
        addIndex(TraitUtils.createCollation(Arrays.stream(keys).boxed().collect(Collectors.toList())), name);

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override
    public void removeIndex(String idxName) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTable table() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory,
            @Nullable BitSet requiredColumns) {
        throw new AssertionError();
    }
}
