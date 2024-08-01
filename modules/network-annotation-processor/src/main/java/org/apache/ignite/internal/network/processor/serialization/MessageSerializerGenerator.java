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

package org.apache.ignite.internal.network.processor.serialization;

import static java.util.stream.Collectors.toList;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transient;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.internal.network.processor.TypeUtils;
import org.apache.ignite.internal.network.serialization.MessageMappingException;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.apache.ignite.internal.network.serialization.MessageWriter;

/**
 * Class for generating {@link MessageSerializer} classes.
 */
public class MessageSerializerGenerator {
    /** Processing environment. */
    private final ProcessingEnvironment processingEnv;

    /** Message Types declarations for the current module. */
    private final MessageGroupWrapper messageGroup;

    private final TypeUtils typeUtils;

    private final MessageWriterMethodResolver methodResolver;

    /** Constructor. */
    public MessageSerializerGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;

        typeUtils = new TypeUtils(processingEnv);
        methodResolver = new MessageWriterMethodResolver(processingEnv);
    }

    /**
     * Generates a {@link MessageSerializer} class for the given network message type.
     *
     * @param message network message
     * @return {@code TypeSpec} of the generated serializer
     */
    public TypeSpec generateSerializer(MessageClass message) {
        processingEnv.getMessager()
                .printMessage(Diagnostic.Kind.NOTE, "Generating a MessageSerializer for " + message.className());

        ClassName serializerClassName = message.serializerClassName();

        return TypeSpec.classBuilder(serializerClassName)
                .addSuperinterface(ParameterizedTypeName.get(ClassName.get(MessageSerializer.class), message.className()))
                .addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PRIVATE)
                        .build())
                .addMethod(writeMessageMethod(message))
                .addField(FieldSpec.builder(serializerClassName, "INSTANCE")
                        .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                        .build()
                )
                .addStaticBlock(CodeBlock.builder()
                        .addStatement("INSTANCE = new $T()", serializerClassName)
                        .build()
                )
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element())
                .build();
    }

    /**
     * Generates the {@link MessageSerializer#writeMessage(NetworkMessage, MessageWriter)} implementation.
     */
    private MethodSpec writeMessageMethod(MessageClass message) {
        MethodSpec.Builder method = MethodSpec.methodBuilder("writeMessage")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addParameter(message.className(), "msg")
                .addParameter(MessageWriter.class, "writer")
                .addException(MessageMappingException.class);

        method.addStatement("$T message = ($T) msg", message.implClassName(), message.implClassName()).addCode("\n");

        List<ExecutableElement> getters = message.getters().stream()
                .filter(e -> e.getAnnotation(Transient.class) == null)
                .collect(toList());

        method
                .beginControlFlow("if (!writer.isHeaderWritten())")
                .beginControlFlow(
                        "if (!writer.writeHeader(message.groupType(), message.messageType(), (byte) $L))", getters.size()
                )
                .addStatement("return false")
                .endControlFlow()
                .addStatement("writer.onHeaderWritten()")
                .endControlFlow()
                .addCode("\n");

        if (!getters.isEmpty()) {
            method.beginControlFlow("switch (writer.state())");

            for (int i = 0; i < getters.size(); ++i) {
                method
                        .beginControlFlow("case $L:", i)
                        .addCode(writeMessageCodeBlock(getters.get(i)))
                        .addCode("\n")
                        .addStatement("writer.incrementState()")
                        .endControlFlow()
                        .addComment("Falls through");
            }

            method.endControlFlow().addCode("\n");
        }

        method.addStatement("return true");

        return method.build();
    }

    /** Helper method for resolving a {@link MessageWriter} "write*" call based on the message field type. */
    private CodeBlock writeMessageCodeBlock(ExecutableElement getter) {
        CodeBlock.Builder writerMethodCallBuilder = CodeBlock.builder();

        if (typeUtils.isEnum(getter.getReturnType())) {
            String fieldName = getter.getSimpleName().toString();

            // Let's write the shifted transferableId to efficiently transfer null (since we use "var int").
            writerMethodCallBuilder
                    .add("int transferableIdShifted = message.$L() == null ? 0 : message.$L().transferableId() + 1;", fieldName, fieldName)
                    .add("\n")
                    .add("boolean written = writer.writeInt($S, transferableIdShifted)", fieldName);
        } else {
            writerMethodCallBuilder
                    .add("boolean written = writer.")
                    .add(methodResolver.resolveWriteMethod(getter));
        }

        return CodeBlock.builder()
                .addStatement(writerMethodCallBuilder.build())
                .add("\n")
                .beginControlFlow("if (!written)")
                .addStatement("return false")
                .endControlFlow()
                .build();
    }
}
