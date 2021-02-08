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
import com.facebook.presto.bytecode.instruction.InstructionNode;
import java.util.List;

import static com.facebook.presto.bytecode.ArrayOpCode.getArrayOpCode;
import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

class SetArrayElementBytecodeExpression
    extends BytecodeExpression {
    private final BytecodeExpression instance;
    private final BytecodeExpression index;
    private final BytecodeExpression value;
    private final InstructionNode arrayStoreInstruction;

    SetArrayElementBytecodeExpression(BytecodeExpression instance, BytecodeExpression index,
        BytecodeExpression value) {
        super(type(void.class));

        this.instance = requireNonNull(instance, "instance is null");
        this.index = requireNonNull(index, "index is null");
        this.value = requireNonNull(value, "value is null");

        ParameterizedType componentType = instance.getType().getArrayComponentType();
        checkArgument(index.getType().getPrimitiveType() == int.class, "index must be int type, but is " + index.getType());
        checkArgument(componentType.equals(value.getType()), "value must be %s type, but is %s", componentType, value.getType());

        this.arrayStoreInstruction = getArrayOpCode(componentType).getStore();
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext) {
        return new BytecodeBlock()
            .append(instance.getBytecode(generationContext))
            .append(index)
            .append(value)
            .append(arrayStoreInstruction);
    }

    @Override
    protected String formatOneLine() {
        return instance + "[" + index + "] = " + value;
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of(index, value);
    }
}
