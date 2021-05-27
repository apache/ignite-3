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
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.processor.internal.MessageClass;
import org.apache.ignite.network.processor.internal.MessageTypes;

/**
 * Class for generating factories for Network Messages inside the given module.
 */
public class MessageFactoryGenerator {
    /** */
    private final ProcessingEnvironment processingEnvironment;

    /**
     * A collection of all annotated Network Messages that are being processed during the current round.
     */
    private final List<MessageClass> messages;

    /**
     * Message Types declarations for the current module.
     */
    private final MessageTypes messageTypes;

    /** */
    public MessageFactoryGenerator(
        ProcessingEnvironment processingEnvironment,
        List<MessageClass> messages,
        MessageTypes messageTypes
    ) {
        this.processingEnvironment = processingEnvironment;
        this.messages = messages;
        this.messageTypes = messageTypes;
    }

    /**
     * Generates a factory for all Network Messages inside the given module.
     */
    public TypeSpec generateMessageFactory() {
        ClassName factoryName = messageTypes.messageFactoryClassName();

        processingEnvironment.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating " + factoryName);

        List<MethodSpec> buildMethods = messages.stream()
            .map(message ->
                MethodSpec.methodBuilder(message.asMethodName())
                    .addModifiers(Modifier.PUBLIC)
                    .returns(message.builderClassName())
                    .addStatement("return $T.builder()", message.implClassName())
                    .build()
            )
            .collect(Collectors.toList());

        return TypeSpec.classBuilder(factoryName)
            .addModifiers(Modifier.PUBLIC)
            .addMethods(buildMethods)
            .build();
    }
}
