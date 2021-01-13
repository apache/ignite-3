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
import com.facebook.presto.bytecode.instruction.LabelNode;
import java.util.List;

import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

class AndBytecodeExpression
    extends BytecodeExpression {
    private final BytecodeExpression left;
    private final BytecodeExpression right;

    AndBytecodeExpression(BytecodeExpression left, BytecodeExpression right) {
        super(type(boolean.class));
        this.left = requireNonNull(left, "left is null");
        checkArgument(left.getType().getPrimitiveType() == boolean.class, "Expected left to be type boolean but is %s", left.getType());
        this.right = requireNonNull(right, "right is null");
        checkArgument(right.getType().getPrimitiveType() == boolean.class, "Expected right to be type boolean but is %s", right.getType());
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext) {
        LabelNode falseLabel = new LabelNode("false");
        LabelNode endLabel = new LabelNode("end");
        return new BytecodeBlock()
            .append(left)
            .ifFalseGoto(falseLabel)
            .append(right)
            .ifFalseGoto(falseLabel)
            .push(true)
            .gotoLabel(endLabel)
            .visitLabel(falseLabel)
            .push(false)
            .visitLabel(endLabel);
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of(left, right);
    }

    @Override
    protected String formatOneLine() {
        return "(" + left + " && " + right + ")";
    }
}
