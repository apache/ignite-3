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

package com.facebook.presto.bytecode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.InsnNode;

import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.toAccessModifier;
import static com.facebook.presto.bytecode.BytecodeUtils.checkArgument;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static org.objectweb.asm.Opcodes.RETURN;

@SuppressWarnings("UnusedDeclaration")
public class MethodDefinition {
    private final Scope scope;
    private final ClassDefinition declaringClass;
    private final EnumSet<Access> access;
    private final String name;
    private final List<AnnotationDefinition> annotations = new ArrayList<>();
    private final ParameterizedType returnType;
    private final List<Parameter> parameters;
    private final List<ParameterizedType> parameterTypes;
    private final List<List<AnnotationDefinition>> parameterAnnotations;
    private final List<ParameterizedType> exceptions = new ArrayList<>();

    private final BytecodeBlock body;
    private String comment;

    public MethodDefinition(
        ClassDefinition declaringClass,
        EnumSet<Access> access,
        String name,
        ParameterizedType returnType,
        Parameter... parameters) {
        this(declaringClass, access, name, returnType, List.of(parameters));
    }

    public MethodDefinition(
        ClassDefinition declaringClass,
        EnumSet<Access> access,
        String name,
        ParameterizedType returnType,
        Collection<Parameter> parameters) {
        checkArgument(parameters.size() <= 254, "Too many parameters for method");

        this.declaringClass = declaringClass;
        body = new BytecodeBlock();

        this.access = access;
        this.name = name;
        this.returnType = returnType != null ? returnType : type(void.class);
        this.parameters = List.copyOf(parameters);
        this.parameterTypes = parameters.stream().map(Parameter::getType).collect(Collectors.toList());
        this.parameterAnnotations = parameters.stream().map(p -> new ArrayList<AnnotationDefinition>()).collect(Collectors.toList());
        Optional<ParameterizedType> thisType = Optional.empty();
        if (!declaringClass.isInterface() && !access.contains(STATIC)) {
            thisType = Optional.of(declaringClass.getType());
        }
        scope = new Scope(thisType, parameters);
    }

    public ClassDefinition getDeclaringClass() {
        return declaringClass;
    }

    public List<AnnotationDefinition> getAnnotations() {
        return List.copyOf(annotations);
    }

    public List<AnnotationDefinition> getParameterAnnotations(int index) {
        return List.copyOf(parameterAnnotations.get(index));
    }

    public EnumSet<Access> getAccess() {
        return access;
    }

    public String getName() {
        return name;
    }

    public ParameterizedType getReturnType() {
        return returnType;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public List<ParameterizedType> getParameterTypes() {
        return parameterTypes;
    }

    public List<ParameterizedType> getExceptions() {
        return exceptions;
    }

    public MethodDefinition addException(Class<? extends Throwable> exceptionClass) {
        exceptions.add(type(exceptionClass));
        return this;
    }

    public MethodDefinition comment(String format, Object... args) {
        this.comment = String.format(format, args);
        return this;
    }

    public String getComment() {
        return comment;
    }

    public Scope getScope() {
        return scope;
    }

    public Variable getThis() {
        return scope.getThis();
    }

    public String getMethodDescriptor() {
        return methodDescription(returnType, parameterTypes);
    }

    public BytecodeBlock getBody() {
        if (declaringClass.isInterface()) {
            throw new IllegalAccessError("Interface does not have method body");
        }
        return body;
    }

    public AnnotationDefinition declareAnnotation(Class<?> type) {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        annotations.add(annotationDefinition);
        return annotationDefinition;
    }

    public AnnotationDefinition declareAnnotation(ParameterizedType type) {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        annotations.add(annotationDefinition);
        return annotationDefinition;
    }

    public AnnotationDefinition declareParameterAnnotation(Class<?> type, int parameterIndex) {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        parameterAnnotations.get(parameterIndex).add(annotationDefinition);
        return annotationDefinition;
    }

    public AnnotationDefinition declareParameterAnnotation(ParameterizedType type, int parameterIndex) {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        parameterAnnotations.get(parameterIndex).add(annotationDefinition);
        return annotationDefinition;
    }

    public void visit(ClassVisitor visitor) {
        visit(visitor, false);
    }

    public void visit(ClassVisitor visitor, boolean addReturn) {
        String[] exceptions = new String[this.exceptions.size()];
        for (int i = 0; i < exceptions.length; i++) {
            exceptions[i] = this.exceptions.get(i).getClassName();
        }

        MethodVisitor methodVisitor = visitor.visitMethod(toAccessModifier(access),
            name,
            getMethodDescriptor(),
            genericMethodSignature(returnType, parameterTypes),
            exceptions);

        if (methodVisitor == null) {
            return;
        }

        // visit method annotations
        for (AnnotationDefinition annotation : annotations) {
            annotation.visitMethodAnnotation(methodVisitor);
        }

        // visit parameter annotations
        for (int parameterIndex = 0; parameterIndex < parameterAnnotations.size(); parameterIndex++) {
            List<AnnotationDefinition> parameterAnnotations1 = this.parameterAnnotations.get(parameterIndex);
            for (AnnotationDefinition parameterAnnotation : parameterAnnotations1) {
                parameterAnnotation.visitParameterAnnotation(parameterIndex, methodVisitor);
            }
        }
        if (!declaringClass.isInterface()) {
            // visit code
            methodVisitor.visitCode();

            // visit instructions
            MethodGenerationContext generationContext = new MethodGenerationContext(methodVisitor);
            generationContext.enterScope(scope);
            body.accept(methodVisitor, generationContext);
            if (addReturn) {
                new InsnNode(RETURN).accept(methodVisitor);
            }
            generationContext.exitScope(scope);
        }
        // done
        methodVisitor.visitMaxs(-1, -1);
        methodVisitor.visitEnd();
    }

    public String toSourceString() {
        StringBuilder sb = new StringBuilder();
        sb.append(access.stream().map(Access::toString).collect(Collectors.joining(" ")))
            .append(' ');
        sb.append(returnType.getJavaClassName()).append(' ');
        sb.append(name).append('(');
        sb.append(parameters.stream().map(Parameter::getSourceString).collect(Collectors.joining(", ")))
            .append(')');
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSourceString();
    }

    public static String methodDescription(Class<?> returnType, Class<?>... parameterTypes) {
        return methodDescription(returnType, List.of(parameterTypes));
    }

    public static String methodDescription(Class<?> returnType, List<Class<?>> parameterTypes) {
        return methodDescription(
            type(returnType),
            parameterTypes.stream().map(ParameterizedType::type).collect(Collectors.toList()));
    }

    public static String methodDescription(
        ParameterizedType returnType,
        ParameterizedType... parameterTypes) {
        return methodDescription(returnType, List.of(parameterTypes));
    }

    public static String methodDescription(
        ParameterizedType returnType,
        List<ParameterizedType> parameterTypes) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(parameterTypes.stream().map(ParameterizedType::getType).collect(Collectors.joining("")));
        sb.append(")");
        sb.append(returnType.getType());
        return sb.toString();
    }

    public static String genericMethodSignature(
        ParameterizedType returnType,
        ParameterizedType... parameterTypes) {
        return genericMethodSignature(returnType, List.of(parameterTypes));
    }

    public static String genericMethodSignature(
        ParameterizedType returnType,
        List<ParameterizedType> parameterTypes) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(parameterTypes.stream().map(ParameterizedType::toString).collect(Collectors.joining("")));
        sb.append(")");
        sb.append(returnType);
        return sb.toString();
    }
}
