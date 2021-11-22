package org.apache.ignite.internal.processors.query.calcite.extension;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 * A handy class to introduce a new convention to the engine via Extension API.
 */
public class ExternalConvention extends Convention.Impl {
    /**
     * Constructor.
     *
     * @param name     The name of the convention. Must be equal to {@link SqlExtension#name()}.
     * @param relClass The interface (usually, but could be a class as well) every relation of given convention have to implement.
     */
    public ExternalConvention(String name, Class<? extends IgniteRel> relClass) {
        super(name, relClass);
    }

    /** {@inheritDoc} */
    @Override
    public RelNode enforce(RelNode rel, RelTraitSet toTraits) {
        return TraitUtils.enforce(rel, toTraits);
    }

    /** {@inheritDoc} */
    @Override
    public ConventionTraitDef getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return toConvention == IgniteConvention.INSTANCE;
    }
}
