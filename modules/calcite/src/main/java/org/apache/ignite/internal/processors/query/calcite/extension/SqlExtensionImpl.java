package org.apache.ignite.internal.processors.query.calcite.extension;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.FilterNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.ScanNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptorImpl;
import org.apache.ignite.internal.schema.NativeTypes;
import org.jetbrains.annotations.Nullable;

/**
 * Asd.
 */
public class SqlExtensionImpl implements SqlExtension {
    /**
     * Asd.
     */
    public static volatile List<String> allNodes;

    private static final String ONLY_TABLE_NAME = "TEST_TBL";

    private static final String ONLY_SCHEMA_NAME = "CUSTOM_SCHEMA";

    @Override
    public void init(Ignite ignite, CatalogUpdateListener catalogUpdateListener) {
        catalogUpdateListener.onCatalogUpdated(new ExternalCatalogImpl());
    }

    /** {@inheritDoc} */
    @Override
    public Set<? extends RelOptRule> getOptimizerRules(PlannerPhase phase) {
        if (phase != PlannerPhase.OPTIMIZATION) {
            return Set.of();
        }

        return Set.of(FilterConverterRule.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return "MY_PLUGIN";
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RelImplementor<RowT> implementor() {
        return new RelImplementor<>() {
            @Override
            public Node<RowT> implement(ExecutionContext<RowT> ctx, IgniteRel node) {
                if (node instanceof MyPhysFilter) {
                    return implement(ctx, (MyPhysFilter) node);
                }

                if (node instanceof MyPhysTableScan) {
                    return implement(ctx, (MyPhysTableScan) node);
                }

                assert false;

                return null;
            }

            private Node<RowT> implement(ExecutionContext<RowT> ctx, MyPhysTableScan scan) {
                RowHandler.RowFactory<RowT> factory = ctx.rowHandler().factory(ctx.getTypeFactory(), scan.getRowType());

                return new ScanNode<>(
                        ctx, scan.getRowType(), List.of(
                        factory.create(1, UUID.randomUUID().toString()),
                        factory.create(2, UUID.randomUUID().toString()),
                        factory.create(3, UUID.randomUUID().toString()),
                        factory.create(4, UUID.randomUUID().toString()),
                        factory.create(5, UUID.randomUUID().toString()),
                        factory.create(6, UUID.randomUUID().toString())
                )
                );
            }

            private Node<RowT> implement(ExecutionContext<RowT> ctx, MyPhysFilter filter) {
                Predicate<RowT> pred = ctx.expressionFactory().predicate(filter.getCondition(), filter.getRowType());

                FilterNode<RowT> node = new FilterNode<>(ctx, filter.getRowType(), pred);

                Node<RowT> input = implement(ctx, (MyPhysFilter) filter.getInput());

                node.register(input);

                return node;
            }
        };
    }

    @Override
    public ColocationGroup colocationGroup(IgniteRel node) {
        return ColocationGroup.forNodes(allNodes);
    }

    private static class ExternalCatalogImpl implements ExternalCatalog {
        private final Map<String, ExternalSchema> schemas = Map.of(ONLY_SCHEMA_NAME, new ExternalSchemaImpl());

        @Override
        public List<String> schemaNames() {
            return List.of(ONLY_SCHEMA_NAME);
        }

        @Override
        public @Nullable ExternalSchema schema(String name) {
            return schemas.get(name);
        }
    }

    private static class ExternalSchemaImpl implements ExternalSchema {
        private final Map<String, IgniteTable> tables = Map.of(ONLY_TABLE_NAME, new OnlyTableImpl(
                new TableDescriptorImpl(
                        List.of(
                                new ColumnDescriptorImpl("C1", true, 0, NativeTypes.INT32, null),
                                new ColumnDescriptorImpl("C2", true, 1, NativeTypes.stringOf(256), null)
                        )
                )
        ));

        @Override
        public List<String> tableNames() {
            return List.of(ONLY_TABLE_NAME);
        }

        @Override
        public @Nullable IgniteTable table(String name) {
            return tables.get(name);
        }
    }
}
