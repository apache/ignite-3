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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.Map;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.tools.Diagnostic;
import org.apache.ignite.internal.network.processor.MessageClass;
import org.apache.ignite.internal.network.processor.MessageGroupWrapper;
import org.apache.ignite.internal.network.serialization.MessageSerializationFactory;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;

/**
 * Class for generating classes for registering all generated {@link MessageSerializationFactory} implementations in a {@link
 * MessageSerializationRegistry}.
 *
 * <p>It is expected that only a single class will be generated for each module that declares any type of network messages.
 */
public class RegistryInitializerGenerator {
    /** Processing environment. */
    private final ProcessingEnvironment processingEnv;

    /** Message group. */
    private final MessageGroupWrapper messageGroup;

    /**
     * Constructor.
     *
     * @param processingEnv Processing environment.
     * @param messageGroup  Message group.
     */
    public RegistryInitializerGenerator(ProcessingEnvironment processingEnv, MessageGroupWrapper messageGroup) {
        this.processingEnv = processingEnv;
        this.messageGroup = messageGroup;
    }

    /**
     * Generates a class for registering all generated {@link MessageSerializationFactory} for the current module.
     *
     * @param messageFactories map from a network message to a corresponding {@code MessageSerializationFactory}
     * @return {@code TypeSpec} of the generated registry initializer
     */
    public TypeSpec generateRegistryInitializer(Map<MessageClass, TypeSpec> messageFactories) {
        String initializerName = messageGroup.groupName() + "SerializationRegistryInitializer";

        processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "Generating " + initializerName);

        TypeSpec.Builder registryInitializer = TypeSpec.classBuilder(initializerName)
                .addSuperinterface(MessageSerializationRegistryInitializer.class);

        MethodSpec.Builder initializeMethod = MethodSpec.methodBuilder("registerFactories")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(TypeName.get(MessageSerializationRegistry.class), "registry")
                .addStatement("var messageFactory = new $T()", messageGroup.messageFactoryClassName())
                .addCode("\n");

        messageFactories.forEach((message, factory) -> {
            var factoryType = ClassName.get(message.packageName(), factory.name);

            initializeMethod.addStatement(
                    "registry.registerFactory($T.GROUP_TYPE, $T.TYPE, new $T(messageFactory))",
                    message.implClassName(), message.implClassName(), factoryType
            );

            registryInitializer.addOriginatingElement(message.element());
        });

        return registryInitializer
                .addModifiers(Modifier.PUBLIC)
                .addMethod(initializeMethod.build())
                .addOriginatingElement(messageGroup.element())
                .build();
    }
}
