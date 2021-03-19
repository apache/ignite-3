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
package com.facebook.presto.bytecode.instruction;

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.BytecodeVisitor;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.OpCode;
import com.facebook.presto.bytecode.ParameterizedType;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static com.facebook.presto.bytecode.MethodDefinition.methodDescription;
import static com.facebook.presto.bytecode.OpCode.INVOKEDYNAMIC;
import static com.facebook.presto.bytecode.OpCode.INVOKEINTERFACE;
import static com.facebook.presto.bytecode.OpCode.INVOKESPECIAL;
import static com.facebook.presto.bytecode.OpCode.INVOKESTATIC;
import static com.facebook.presto.bytecode.OpCode.INVOKEVIRTUAL;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("UnusedDeclaration")
public class InvokeInstruction
    implements InstructionNode {
    //
    // Invoke Static
    //

    public static InstructionNode invokeStatic(Method method) {
        return invoke(INVOKESTATIC, method);
    }

    public static InstructionNode invokeStatic(MethodDefinition method) {
        return invoke(INVOKESTATIC, method);
    }

    public static InstructionNode invokeStatic(Class<?> target, String name, Class<?> returnType,
        Class<?>... parameterTypes) {
        return invoke(INVOKESTATIC, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeStatic(Class<?> target, String name, Class<?> returnType,
        Collection<Class<?>> parameterTypes) {
        return invoke(INVOKESTATIC, target, name, returnType, parameterTypes);
    }

    public static InstructionNode invokeStatic(ParameterizedType target, String name, ParameterizedType returnType,
        ParameterizedType... parameterTypes) {
        return invoke(INVOKESTATIC, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeStatic(ParameterizedType target, String name, ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes) {
        return invoke(INVOKESTATIC, target, name, returnType, parameterTypes);
    }

    //
    // Invoke Virtual
    //

    public static InstructionNode invokeVirtual(Method method) {
        return invoke(INVOKEVIRTUAL, method);
    }

    public static InstructionNode invokeVirtual(MethodDefinition method) {
        return invoke(INVOKEVIRTUAL, method);
    }

    public static InstructionNode invokeVirtual(Class<?> target, String name, Class<?> returnType,
        Class<?>... parameterTypes) {
        return invoke(INVOKEVIRTUAL, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeVirtual(Class<?> target, String name, Class<?> returnType,
        Collection<Class<?>> parameterTypes) {
        return invoke(INVOKEVIRTUAL, target, name, returnType, parameterTypes);
    }

    public static InstructionNode invokeVirtual(ParameterizedType target, String name, ParameterizedType returnType,
        ParameterizedType... parameterTypes) {
        return invoke(INVOKEVIRTUAL, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeVirtual(ParameterizedType target, String name, ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes) {
        return invoke(INVOKEVIRTUAL, target, name, returnType, parameterTypes);
    }

    //
    // Invoke Interface
    //

    public static InstructionNode invokeInterface(Method method) {
        return invoke(INVOKEINTERFACE, method);
    }

    public static InstructionNode invokeInterface(MethodDefinition method) {
        return invoke(INVOKEINTERFACE, method);
    }

    public static InstructionNode invokeInterface(Class<?> target, String name, Class<?> returnType,
        Class<?>... parameterTypes) {
        return invoke(INVOKEINTERFACE, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeInterface(Class<?> target, String name, Class<?> returnType,
        Collection<Class<?>> parameterTypes) {
        return invoke(INVOKEINTERFACE, target, name, returnType, parameterTypes);
    }

    public static InstructionNode invokeInterface(ParameterizedType target, String name, ParameterizedType returnType,
        ParameterizedType... parameterTypes) {
        return invoke(INVOKEINTERFACE, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeInterface(ParameterizedType target, String name, ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes) {
        return invoke(INVOKEINTERFACE, target, name, returnType, parameterTypes);
    }

    //
    // Invoke Constructor
    //

    public static InstructionNode invokeConstructor(Constructor<?> constructor) {
        return invokeConstructor(constructor.getDeclaringClass(), constructor.getParameterTypes());
    }

    public static InstructionNode invokeConstructor(Class<?> target, Class<?>... parameterTypes) {
        return invokeConstructor(type(target), Arrays.stream(parameterTypes).map(ParameterizedType::type).collect(Collectors.toList()));
    }

    public static InstructionNode invokeConstructor(Class<?> target, Collection<Class<?>> parameterTypes) {
        return invokeConstructor(type(target), parameterTypes.stream().map(ParameterizedType::type).collect(Collectors.toList()));
    }

    public static InstructionNode invokeConstructor(ParameterizedType target, ParameterizedType... parameterTypes) {
        return invokeConstructor(target, List.of(parameterTypes));
    }

    public static InstructionNode invokeConstructor(ParameterizedType target,
        Collection<ParameterizedType> parameterTypes) {
        return invokeSpecial(target, "<init>", type(void.class), parameterTypes);
    }

    //
    // Invoke Special
    //

    public static InstructionNode invokeSpecial(Method method) {
        return invoke(INVOKESPECIAL, method);
    }

    public static InstructionNode invokeSpecial(MethodDefinition method) {
        return invoke(INVOKESPECIAL, method);
    }

    public static InstructionNode invokeSpecial(Class<?> target, String name, Class<?> returnType,
        Class<?>... parameterTypes) {
        return invoke(INVOKESPECIAL, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeSpecial(Class<?> target, String name, Class<?> returnType,
        Collection<Class<?>> parameterTypes) {
        return invoke(INVOKESPECIAL, target, name, returnType, parameterTypes);
    }

    public static InstructionNode invokeSpecial(ParameterizedType target, String name, ParameterizedType returnType,
        ParameterizedType... parameterTypes) {
        return invoke(INVOKESPECIAL, target, name, returnType, List.of(parameterTypes));
    }

    public static InstructionNode invokeSpecial(ParameterizedType target, String name, ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes) {
        return invoke(INVOKESPECIAL, target, name, returnType, parameterTypes);
    }

    //
    // Generic
    //

    private static InstructionNode invoke(OpCode invocationType, Method method) {
        return new InvokeInstruction(invocationType,
            type(method.getDeclaringClass()),
            method.getName(),
            type(method.getReturnType()),
            Arrays.stream(method.getParameterTypes()).map(ParameterizedType::type).collect(Collectors.toList()));
    }

    private static InstructionNode invoke(OpCode invocationType, MethodDefinition method) {
        return new InvokeInstruction(invocationType,
            method.getDeclaringClass().getType(),
            method.getName(),
            method.getReturnType(),
            method.getParameterTypes());
    }

    private static InstructionNode invoke(OpCode invocationType, ParameterizedType target, String name,
        ParameterizedType returnType, Collection<ParameterizedType> parameterTypes) {
        return new InvokeInstruction(invocationType,
            target,
            name,
            returnType,
            parameterTypes);
    }

    private static InstructionNode invoke(OpCode invocationType, Class<?> target, String name, Class<?> returnType,
        Collection<Class<?>> parameterTypes) {
        return new InvokeInstruction(invocationType,
            type(target),
            name,
            type(returnType),
            parameterTypes.stream().map(ParameterizedType::type).collect(Collectors.toList()));
    }

    //
    // Invoke Dynamic
    //

    public static InstructionNode invokeDynamic(String name,
        ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes,
        Method bootstrapMethod,
        Collection<Object> bootstrapArguments) {
        return new InvokeDynamicInstruction(name,
            returnType,
            parameterTypes,
            bootstrapMethod,
            List.copyOf(bootstrapArguments));
    }

    public static InstructionNode invokeDynamic(String name,
        ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes,
        Method bootstrapMethod,
        Object... bootstrapArguments) {
        return new InvokeDynamicInstruction(name,
            returnType,
            parameterTypes,
            bootstrapMethod,
            List.of(bootstrapArguments));
    }

    public static InstructionNode invokeDynamic(String name,
        MethodType methodType,
        Method bootstrapMethod,
        Collection<Object> bootstrapArguments) {
        return new InvokeDynamicInstruction(name,
            type(methodType.returnType()),
            methodType.parameterList().stream().map(ParameterizedType::type).collect(Collectors.toList()),
            bootstrapMethod,
            List.copyOf(bootstrapArguments));
    }

    public static InstructionNode invokeDynamic(String name,
        MethodType methodType,
        Method bootstrapMethod,
        Object... bootstrapArguments) {
        return new InvokeDynamicInstruction(name,
            type(methodType.returnType()),
            methodType.parameterList().stream().map(ParameterizedType::type).collect(Collectors.toList()),
            bootstrapMethod,
            List.of(bootstrapArguments));
    }

    private final OpCode opCode;
    private final ParameterizedType target;
    private final String name;
    private final ParameterizedType returnType;
    private final List<ParameterizedType> parameterTypes;

    public InvokeInstruction(OpCode opCode,
        ParameterizedType target,
        String name,
        ParameterizedType returnType,
        Collection<ParameterizedType> parameterTypes) {
        checkUnqualifiedName(name);
        this.opCode = opCode;
        this.target = target;
        this.name = name;
        this.returnType = returnType;
        this.parameterTypes = List.copyOf(parameterTypes);
    }

    public OpCode getOpCode() {
        return opCode;
    }

    public ParameterizedType getTarget() {
        return target;
    }

    public String getName() {
        return name;
    }

    public ParameterizedType getReturnType() {
        return returnType;
    }

    public List<ParameterizedType> getParameterTypes() {
        return parameterTypes;
    }

    public String getMethodDescription() {
        return methodDescription(returnType, parameterTypes);
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext) {
        visitor.visitMethodInsn(opCode.getOpCode(), target.getClassName(), name, getMethodDescription(), target.isInterface());
    }

    @Override
    public List<BytecodeNode> getChildNodes() {
        return List.of();
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor) {
        return visitor.visitInvoke(parent, this);
    }

    public static class InvokeDynamicInstruction
        extends InvokeInstruction {
        private final Method bootstrapMethod;
        private final List<Object> bootstrapArguments;

        public InvokeDynamicInstruction(String name,
            ParameterizedType returnType,
            Collection<ParameterizedType> parameterTypes,
            Method bootstrapMethod,
            List<Object> bootstrapArguments) {
            super(INVOKEDYNAMIC, null, name, returnType, parameterTypes);
            this.bootstrapMethod = bootstrapMethod;
            this.bootstrapArguments = List.copyOf(bootstrapArguments);
        }

        @Override
        public void accept(MethodVisitor visitor, MethodGenerationContext generationContext) {
            Handle bootstrapMethodHandle = new Handle(Opcodes.H_INVOKESTATIC,
                type(bootstrapMethod.getDeclaringClass()).getClassName(),
                bootstrapMethod.getName(),
                methodDescription(
                    bootstrapMethod.getReturnType(),
                    bootstrapMethod.getParameterTypes()),
                false);

            visitor.visitInvokeDynamicInsn(getName(),
                getMethodDescription(),
                bootstrapMethodHandle,
                bootstrapArguments.toArray());
        }

        public Method getBootstrapMethod() {
            return bootstrapMethod;
        }

        public List<Object> getBootstrapArguments() {
            return bootstrapArguments;
        }

        @Override
        public List<BytecodeNode> getChildNodes() {
            return List.of();
        }

        @Override
        public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor) {
            return visitor.visitInvokeDynamic(parent, this);
        }
    }

    private static void checkUnqualifiedName(String name) {
        // JVM Specification 4.2.2 Unqualified Names
        requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        if ("<init>".equals(name) || "<clinit>".equals(name)) {
            return;
        }

        checkArgument(!name.matches(".*[.;\\[/<>].*"), "invalid name: %s", name);
    }
}
