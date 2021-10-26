/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.extension;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 * Ignite convention trait.
 */
public class MyConvention extends Convention.Impl {
    /**
     * Asd.
     */
    public static final MyConvention INSTANCE = new MyConvention();

    /**
     * Asd.
     */
    private MyConvention() {
        super("MY_IGNITE", RelNode.class);
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
