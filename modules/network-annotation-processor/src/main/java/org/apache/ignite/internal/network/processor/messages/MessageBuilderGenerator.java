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

package org.apache.ignite.internal.network.processor.messages;

import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.BYTE_ARRAY_TYPE;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.NULLABLE_ANNOTATION_SPEC;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.NULLABLE_BYTE_ARRAY_TYPE;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.addByteArrayPostfix;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.methodReturnsNullableValue;
import static org.apache.ignite.internal.network.processor.MessageGeneratorUtils.methodReturnsPrimitive;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;

/**
 * Class for generating Builder interfaces for Network Messages.
 */
public class MessageBuilderGenerator {
    /** Processing environment. */
    private final ProcessingEnvironment processingEnvironment;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    /**
     * Constructor.
     *
     * @param processingEnvironment Processing environment.
     * @param messageGroup          Message group.
     */
    public MessageBuilderGenerator(ProcessingEnvironment processingEnvironment, MessageGroupWrapper messageGroup) {
        this.processingEnvironment = processingEnvironment;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a Builder interface for constructing the given Network Message.
     *
     * @param message network message
     * @return {@code TypeSpec} of the generated Builder interface
     */
    public TypeSpec generateBuilderInterface(MessageClass message) {
        ClassName builderName = message.builderClassName();

        processingEnvironment.getMessager()
                .printMessage(Diagnostic.Kind.NOTE, "Generating " + builderName);

        MethodSpec buildMethod = MethodSpec.methodBuilder("build")
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                .returns(message.className())
                .build();

        return TypeSpec.interfaceBuilder(builderName)
                .addModifiers(Modifier.PUBLIC)
                .addMethods(generateGettersAndSetters(message))
                .addMethods(generateByteArrayGettersAndSetters(message))
                .addMethod(buildMethod)
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element())
                .build();
    }

    private static List<MethodSpec> generateGettersAndSetters(MessageClass message) {
        var methods = new ArrayList<MethodSpec>();

        for (ExecutableElement networkMessageGetter : message.getters()) {
            String fieldName = networkMessageGetter.getSimpleName().toString();

            TypeName returnTypeName = TypeName.get(networkMessageGetter.getReturnType());

            if (methodReturnsNotPrimitiveButNullableValue(networkMessageGetter)) {
                // Allows us to generate (for example):
                // TestMessageBuilder value(@Nullable String value);
                // @Nullable String value();
                // TestMessageBuilder values(String @Nullable [] value);
                // String @Nullable [] values();
                returnTypeName = returnTypeName.annotated(NULLABLE_ANNOTATION_SPEC);
            }

            // generate a setter for each getter in the original interface
            MethodSpec setterSpec = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addParameter(returnTypeName, fieldName)
                    .returns(message.builderClassName())
                    .build();

            // generate a getter for each getter in the original interface
            MethodSpec getterSpec = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(returnTypeName)
                    .build();

            methods.add(setterSpec);
            methods.add(getterSpec);
        }

        return methods;
    }

    private static List<MethodSpec> generateByteArrayGettersAndSetters(MessageClass message) {
        var methods = new ArrayList<MethodSpec>();

        for (ExecutableElement networkMessageGetter : message.getters()) {
            if (networkMessageGetter.getAnnotation(Marshallable.class) != null) {
                String fieldName = networkMessageGetter.getSimpleName().toString();

                TypeName getterAndSetterTypeName = BYTE_ARRAY_TYPE;

                if (methodReturnsNotPrimitiveButNullableValue(networkMessageGetter)) {
                    // Allows us to generate (for example):
                    // TestMessageBuilder valueByteArray(byte @Nullable [] value);
                    // byte @Nullable [] valueByteArray();
                    getterAndSetterTypeName = NULLABLE_BYTE_ARRAY_TYPE;
                }

                MethodSpec baSetter = MethodSpec.methodBuilder(addByteArrayPostfix(fieldName))
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addParameter(getterAndSetterTypeName, addByteArrayPostfix(fieldName))
                        .returns(message.builderClassName())
                        .build();

                MethodSpec baGetter = MethodSpec.methodBuilder(addByteArrayPostfix(fieldName))
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .returns(getterAndSetterTypeName)
                        .build();

                methods.add(baSetter);
                methods.add(baGetter);
            }
        }

        return methods;
    }

    private static boolean methodReturnsNotPrimitiveButNullableValue(ExecutableElement method) {
        return !methodReturnsPrimitive(method) && methodReturnsNullableValue(method);
    }
}
