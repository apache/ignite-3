/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.externalize;

import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.NotNull;

class IgniteRelJsonTypesCache {

    private final List<String> packages;
    private final LoadingCache<@NotNull String, RelFactory> factoriesCache;

    IgniteRelJsonTypesCache(List<String> packages) {
        this.packages = packages;
        this.factoriesCache = Caffeine.newBuilder()
                .build(this::relFactory);
    }

    @FunctionalInterface
    public interface RelFactory extends Function<RelInput, RelNode> {
        /** {@inheritDoc} */
        @Override
        RelNode apply(RelInput input);
    }

    @SuppressWarnings("DataFlowIssue")
    Function<RelInput, RelNode> factory(String type) {
        return factoriesCache.get(type);
    }

    @SuppressWarnings("NestedAssignment")
    private RelFactory relFactory(String typeName) {
        Class<?> clazz = null;

        if (!typeName.contains(".")) {
            for (String pkg : packages) {
                if ((clazz = IgniteRelJsonUtils.classForNameOrNull(pkg + typeName)) != null) {
                    break;
                }
            }
        }

        if (clazz == null) {
            clazz = IgniteRelJsonUtils.classForName(typeName);
        }

        assert RelNode.class.isAssignableFrom(clazz);

        Constructor<RelNode> constructor;

        try {
            constructor = (Constructor<RelNode>) clazz.getConstructor(RelInput.class);
        } catch (NoSuchMethodException ignored) {
            throw new IgniteInternalException(INTERNAL_ERR, "class does not have required constructor, "
                    + clazz + "(RelInput)");
        }

        BlockBuilder builder = new BlockBuilder();
        ParameterExpression input = Expressions.parameter(RelInput.class);
        builder.add(Expressions.new_(constructor, input));
        MethodDeclaration declaration = Expressions.methodDecl(
                Modifier.PUBLIC, RelNode.class, "apply", asList(input), builder.toBlock());
        return Commons.compile(RelFactory.class, Expressions.toString(asList(declaration), "\n", true));
    }
}
