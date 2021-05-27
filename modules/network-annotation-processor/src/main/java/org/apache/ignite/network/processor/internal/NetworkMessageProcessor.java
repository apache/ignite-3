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

package org.apache.ignite.network.processor.internal;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.AutoMessage;
import org.apache.ignite.network.annotations.ModuleMessageTypes;
import org.apache.ignite.network.processor.internal.messages.MessageBuilderGenerator;
import org.apache.ignite.network.processor.internal.messages.MessageFactoryGenerator;
import org.apache.ignite.network.processor.internal.messages.MessageImplGenerator;
import org.apache.ignite.network.processor.internal.serialization.MessageDeserializerGenerator;
import org.apache.ignite.network.processor.internal.serialization.MessageSerializerGenerator;
import org.apache.ignite.network.processor.internal.serialization.RegistryInitializerGenerator;
import org.apache.ignite.network.processor.internal.serialization.SerializationFactoryGenerator;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Annotation processor for working with the {@link AutoMessage} annotation.
 */
public class NetworkMessageProcessor extends AbstractProcessor {
    /** {@inheritDoc} */
    @Override public Set<String> getSupportedAnnotationTypes() {
        return Set.of(AutoMessage.class.getName());
    }

    /** {@inheritDoc} */
    @Override public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /** {@inheritDoc} */
    @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        List<MessageClass> messages = annotations.stream()
            .map(roundEnv::getElementsAnnotatedWith)
            .flatMap(Collection::stream)
            .map(TypeElement.class::cast)
            .map(MessageClass::new)
            .collect(Collectors.toList());

        if (messages.isEmpty()) {
            return true;
        }

        try {
            validateMessages(messages);

            MessageTypes moduleMessageTypes = getModuleMessageTypes(roundEnv);

            generateMessageImpls(messages, moduleMessageTypes);

            generateSerializers(messages, moduleMessageTypes);
        }
        catch (ProcessingException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), e.getElement());
        }

        return true;
    }

    /**
     * Generates the following classes for the current compilation unit:
     *
     * <ol>
     *     <li>Builder interfaces;</li>
     *     <li>Network Message and Builder implementations;</li>
     *     <li>Message factory for all generated messages.</li>
     * </ol>
     */
    private void generateMessageImpls(List<MessageClass> annotatedMessages, MessageTypes messageTypes) {
        for (MessageClass message : annotatedMessages) {
            try {
                // generate a Builder interface with setters
                TypeSpec builder = new MessageBuilderGenerator(processingEnv, message)
                    .generateBuilderInterface();

                writeToFile(message.packageName(), builder);

                // generate the message and the builder implementations
                TypeSpec messageImpl = new MessageImplGenerator(processingEnv, message, builder, messageTypes)
                    .generateMessageImpl();

                writeToFile(message.packageName(), messageImpl);
            }
            catch (ProcessingException e) {
                throw new ProcessingException(e.getMessage(), e.getCause(), message.element());
            }
        }

        // generate a factory for all messages inside the current compilation unit
        TypeSpec messageFactory = new MessageFactoryGenerator(processingEnv, annotatedMessages, messageTypes)
            .generateMessageFactory();

        writeToFile(messageTypes.packageName(), messageFactory);
    }

    /**
     * Generates the following classes for the current compilation unit:
     *
     * <ol>
     *     <li>{@link MessageSerializer};</li>
     *     <li>{@link MessageDeserializer};</li>
     *     <li>{@link MessageSerializationFactory};</li>
     *     <li>Helper class for adding all generated serialization factories to a
     *     {@link MessageSerializationRegistry}.</li>
     * </ol>
     */
    private void generateSerializers(List<MessageClass> annotatedMessages, MessageTypes messageTypes) {
        var factories = new HashMap<MessageClass, TypeSpec>();

        annotatedMessages.stream()
            .filter(MessageClass::isAutoSerializable)
            .forEach(message -> {
                try {
                    // MessageSerializer
                    TypeSpec serializer = new MessageSerializerGenerator(processingEnv, message)
                        .generateSerializer();

                    writeToFile(message.packageName(), serializer);

                    // MessageDeserializer
                    TypeSpec deserializer = new MessageDeserializerGenerator(processingEnv, message, messageTypes)
                        .generateDeserializer();

                    writeToFile(message.packageName(), deserializer);

                    // MessageSerializationFactory
                    TypeSpec factory = new SerializationFactoryGenerator(processingEnv, message, messageTypes)
                        .generateFactory(serializer, deserializer);

                    writeToFile(message.packageName(), factory);

                    factories.put(message, factory);
                }
                catch (ProcessingException e) {
                    throw new ProcessingException(e.getMessage(), e.getCause(), message.element());
                }
            });

        if (!factories.isEmpty()) {
            TypeSpec registryInitializer = new RegistryInitializerGenerator(processingEnv)
                .generateRegistryInitializer(factories, messageTypes);

            writeToFile(messageTypes.packageName(), registryInitializer);
        }
    }

    /**
     * Validates the annotated messages:
     *
     * <ol>
     *     <li>{@link AutoMessage} annotation is present on a valid element;</li>
     *     <li>No messages with the same message type exist.</li>
     * </ol>
     */
    private void validateMessages(List<MessageClass> messages) {
        var typeUtils = new TypeUtils(processingEnv);

        var messageTypesSet = new HashSet<Short>();

        for (MessageClass message : messages) {
            TypeElement element = message.element();

            boolean isValid = element.getKind() == ElementKind.INTERFACE
                && typeUtils.hasSuperInterface(element, NetworkMessage.class);

            if (!isValid) {
                var errorMsg = String.format(
                    "%s annotation must only be present on interfaces that extend %s",
                    AutoMessage.class, NetworkMessage.class
                );

                throw new ProcessingException(errorMsg, null, element);
            }

            short messageType = message.messageType();

            if (!messageTypesSet.add(messageType)) {
                var errorMsg = String.format(
                    "Conflicting message types in a module, message with type %d already exists",
                    messageType
                );

                throw new ProcessingException(errorMsg, null, element);
            }
        }
    }

    /**
     * Extracts and validates the declared module message types marked with the {@link ModuleMessageTypes}
     * annotation.
     */
    private static MessageTypes getModuleMessageTypes(RoundEnvironment roundEnv) {
        Set<? extends Element> moduleMessageTypesSet = roundEnv.getElementsAnnotatedWith(ModuleMessageTypes.class);

        if (moduleMessageTypesSet.size() != 1) {
            var errorMsg = "Invalid number of module message types classes (annotated with @ModuleMessageTypes): " +
                moduleMessageTypesSet.size();

            throw new ProcessingException(errorMsg);
        }

        Element singleElement = moduleMessageTypesSet.iterator().next();
        return new MessageTypes((TypeElement) singleElement);
    }

    /**
     * Writes the given generated class into a file.
     */
    private void writeToFile(String packageName, TypeSpec typeSpec) {
        try {
            JavaFile
                .builder(packageName, typeSpec)
                .indent(" ".repeat(4))
                .build()
                .writeTo(processingEnv.getFiler());
        } catch (IOException e) {
            throw new ProcessingException("IO exception during annotation processing", e);
        }
    }
}
