/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class NewInstanceBytecodeExpression
        extends BytecodeExpression
{
    private final List<BytecodeExpression> parameters;
    private final List<ParameterizedType> parameterTypes;

    NewInstanceBytecodeExpression(
            ParameterizedType type,
            Collection<ParameterizedType> parameterTypes,
            Collection<? extends BytecodeExpression> parameters)
    {
        super(type);
        this.parameterTypes = List.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        this.parameters = List.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        BytecodeBlock block = new BytecodeBlock()
                .newObject(getType())
                .dup();

        for (BytecodeExpression parameter : parameters) {
            block.append(parameter);
        }
        return block.invokeConstructor(getType(), parameterTypes);
    }

    @Override
    protected String formatOneLine()
    {
        return "new " + getType().getSimpleName() + "(" +
            parameters.stream().map(BytecodeExpression::toString).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return List.copyOf(parameters);
    }
}
