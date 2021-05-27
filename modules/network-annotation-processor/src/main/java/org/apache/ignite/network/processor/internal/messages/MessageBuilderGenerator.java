/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.network.processor.internal.messages;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.processor.internal.MessageClass;

/**
 * Class for generating Builder interfaces for Network Messages.
 */
public class MessageBuilderGenerator {
    /** */
    private final ProcessingEnvironment processingEnvironment;

    /** Network message element. */
    private final MessageClass message;

    /** */
    public MessageBuilderGenerator(ProcessingEnvironment processingEnvironment, MessageClass message) {
        this.processingEnvironment = processingEnvironment;
        this.message = message;
    }

    /**
     * Generates a Builder interface for constructing the given Network Message.
     */
    public TypeSpec generateBuilderInterface() {
        ClassName builderName = message.builderClassName();

        processingEnvironment.getMessager()
            .printMessage(Diagnostic.Kind.NOTE, "Generating " + builderName, message.element());

        // generate a setter for each getter in the original interface
        List<MethodSpec> setters = message.getters().stream()
            .map(getter -> {
                String getterName = getter.getSimpleName().toString();

                return MethodSpec.methodBuilder(getterName)
                    .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                    .addParameter(TypeName.get(getter.getReturnType()), getterName)
                    .returns(builderName)
                    .build();
            })
            .collect(Collectors.toList());

        MethodSpec buildMethod = MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
            .returns(message.className())
            .build();

        return TypeSpec.interfaceBuilder(builderName)
            .addModifiers(Modifier.PUBLIC)
            .addMethods(setters)
            .addMethod(buildMethod)
            .build();
    }
}
