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

import static org.apache.ignite.internal.network.processor.messages.MessageImplGenerator.BYTE_ARRAY_TYPE;
import static org.apache.ignite.internal.network.processor.messages.MessageImplGenerator.getByteArrayFieldName;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.type.TypeMirror;
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
                .addMethods(generateGettersAndSetters(message, message.getters()))
                .addMethods(generateByteArrayGettersAndSetters(message, message.getters()))
                .addMethod(buildMethod)
                .addOriginatingElement(message.element())
                .addOriginatingElement(messageGroup.element())
                .build();
    }

    private List<MethodSpec> generateGettersAndSetters(MessageClass message, List<ExecutableElement> fields) {
        List<MethodSpec> methods = new ArrayList<>();

        for (ExecutableElement field : fields) {
            String fieldName = field.getSimpleName().toString();

            TypeMirror type = field.getReturnType();

            // generate a setter for each getter in the original interface
            MethodSpec setterSpec = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addParameter(TypeName.get(type), fieldName)
                    .returns(message.builderClassName())
                    .build();

            // generate a getter for each getter in the original interface
            MethodSpec getterSpec = MethodSpec.methodBuilder(fieldName)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .returns(TypeName.get(type))
                    .build();

            methods.add(setterSpec);
            methods.add(getterSpec);
        }

        return methods;
    }

    private List<MethodSpec> generateByteArrayGettersAndSetters(MessageClass message, List<ExecutableElement> fields) {
        List<MethodSpec> methods = new ArrayList<>();

        for (ExecutableElement field : fields) {
            if (field.getAnnotation(Marshallable.class) != null) {
                String fieldName = field.getSimpleName().toString();

                MethodSpec baSetter = MethodSpec.methodBuilder(getByteArrayFieldName(fieldName))
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .addParameter(BYTE_ARRAY_TYPE, getByteArrayFieldName(fieldName))
                        .returns(message.builderClassName())
                        .build();

                MethodSpec baGetter = MethodSpec.methodBuilder(getByteArrayFieldName(fieldName))
                        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                        .returns(BYTE_ARRAY_TYPE)
                        .build();

                methods.add(baSetter);
                methods.add(baGetter);
            }
        }

        return methods;
    }
}
