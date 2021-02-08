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
import com.facebook.presto.bytecode.instruction.TypeInstruction;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

import static com.facebook.presto.bytecode.ArrayOpCode.getArrayOpCode;
import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static java.util.Objects.requireNonNull;

class NewArrayBytecodeExpression
    extends BytecodeExpression {
    private final BytecodeExpression length;
    private final ParameterizedType elementType;

    @Nullable
    private final List<BytecodeExpression> elements;

    NewArrayBytecodeExpression(ParameterizedType type, int length) {
        this(type, constantInt(length));
    }

    NewArrayBytecodeExpression(ParameterizedType type, BytecodeExpression length) {
        this(type, length, null);
    }

    NewArrayBytecodeExpression(ParameterizedType type, Collection<BytecodeExpression> elements) {
        this(type, constantInt(elements.size()), elements);
    }

    private NewArrayBytecodeExpression(ParameterizedType type, BytecodeExpression length,
        Collection<BytecodeExpression> elements) {
        super(type);
        requireNonNull(type, "type is null");
        checkArgument(type.getArrayComponentType() != null, "type %s must be array type", type);
        this.elementType = type.getArrayComponentType();
        this.length = requireNonNull(length, "length is null");
        this.elements = (elements == null) ? null : List.copyOf(elements);
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext) {
        BytecodeBlock bytecodeBlock;
        if (elementType.isPrimitive()) {
            bytecodeBlock = new BytecodeBlock()
                .append(length)
                .append(TypeInstruction.newPrimitiveArray(elementType));
        }
        else {
            bytecodeBlock = new BytecodeBlock()
                .append(length)
                .append(TypeInstruction.newObjectArray(elementType));
        }
        if (elements != null) {
            for (int i = 0; i < elements.size(); i++) {
                BytecodeExpression element = elements.get(i);
                bytecodeBlock
                    .dup()
                    .append(constantInt(i))
                    .append(element)
                    .append(getArrayOpCode(elementType).getStore());
            }
        }
        return bytecodeBlock;
    }

    @Override
    protected String formatOneLine() {
        if (elements == null) {
            return "new " + elementType.getSimpleName() + "[" + length + "]";
        }
        return "new " + elementType.getSimpleName() + "[] {" + elements.stream().map(BytecodeExpression::toString).collect(Collectors.joining(", ")) + "}";
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of();
    }
}
