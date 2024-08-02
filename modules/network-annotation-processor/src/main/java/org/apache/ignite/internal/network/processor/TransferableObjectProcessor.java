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

package org.apache.ignite.internal.network.processor;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.google.auto.service.AutoService;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.network.processor.messages.MessageBuilderGenerator;
import org.apache.ignite.internal.network.processor.messages.MessageFactoryGenerator;
import org.apache.ignite.internal.network.processor.messages.MessageImplGenerator;
import org.apache.ignite.internal.network.processor.serialization.MessageDeserializerGenerator;
import org.apache.ignite.internal.network.processor.serialization.MessageSerializerGenerator;
import org.apache.ignite.internal.network.processor.serialization.RegistryInitializerGenerator;
import org.apache.ignite.internal.network.processor.serialization.SerializationFactoryGenerator;
import org.apache.ignite.internal.network.serialization.MessageDeserializer;
import org.apache.ignite.internal.network.serialization.MessageSerializationFactory;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistryInitializer;
import org.apache.ignite.internal.network.serialization.MessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Annotation processor for working with the {@link Transferable} annotation.
 */
@AutoService(Processor.class)
public class TransferableObjectProcessor extends AbstractProcessor {
    private static final String SPI_FILE_NAME = "META-INF/services/" + MessageSerializationRegistryInitializer.class.getName();

    /** {@inheritDoc} */
    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(Transferable.class.getName());
    }

    /** {@inheritDoc} */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /** {@inheritDoc} */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            Set<MessageClass> messages = annotations.stream()
                    .map(roundEnv::getElementsAnnotatedWith)
                    .flatMap(Collection::stream)
                    .map(TypeElement.class::cast)
                    .map(e -> new MessageClass(processingEnv, e))
                    .collect(toSet());

            if (messages.isEmpty()) {
                return true;
            }

            IncrementalCompilationConfig currentConfig = IncrementalCompilationConfig.readConfig(processingEnv);

            MessageGroupWrapper messageGroup = getMessageGroup(roundEnv, currentConfig);

            if (currentConfig != null) {
                messages = mergeMessages(currentConfig, messages);
            }

            updateConfig(messages, messageGroup);

            validateMessages(messages);

            generateEnumTransferableUtils(messages, messageGroup);

            generateMessageImpls(messages, messageGroup);

            generateSerializers(messages, messageGroup);
        } catch (ProcessingException e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage(), e.getElement());
        }

        return true;
    }

    private void updateConfig(Collection<MessageClass> messages, MessageGroupWrapper messageGroup) {
        List<ClassName> messageClassNames = messages.stream()
                .map(MessageClass::className)
                .collect(toList());

        var config = new IncrementalCompilationConfig(ClassName.get(messageGroup.element()), messageClassNames);

        config.writeConfig(processingEnv);
    }

    private Set<MessageClass> mergeMessages(IncrementalCompilationConfig currentConfig, Collection<MessageClass> messages) {
        Elements elementUtils = processingEnv.getElementUtils();

        Stream<MessageClass> configMessages = currentConfig.messageClasses().stream()
                .map(ClassName::canonicalName)
                .map(elementUtils::getTypeElement)
                .filter(element -> element != null && element.getAnnotation(Transferable.class) != null)
                .map(element -> new MessageClass(processingEnv, element));

        return Stream.concat(messages.stream(), configMessages).collect(toSet());
    }

    /**
     * Generates the following classes for the current compilation unit.
     *
     * <ol>
     *     <li>Builder interfaces;</li>
     *     <li>Network Message and Builder implementations;</li>
     *     <li>Message factory for all generated messages.</li>
     * </ol>
     */
    private void generateMessageImpls(Collection<MessageClass> annotatedMessages, MessageGroupWrapper messageGroup) {
        var messageBuilderGenerator = new MessageBuilderGenerator(processingEnv, messageGroup);
        var messageImplGenerator = new MessageImplGenerator(processingEnv, messageGroup);
        var messageFactoryGenerator = new MessageFactoryGenerator(processingEnv, messageGroup);

        for (MessageClass message : annotatedMessages) {
            try {
                // generate a Builder interface with setters
                TypeSpec builder = messageBuilderGenerator.generateBuilderInterface(message);

                writeToFile(message.packageName(), builder);

                // generate the message and the builder implementations
                TypeSpec messageImpl = messageImplGenerator.generateMessageImpl(message, builder);

                writeToFile(message.packageName(), messageImpl);
            } catch (ProcessingException e) {
                throw new ProcessingException(e.getMessage(), e.getCause(), message.element());
            }
        }

        // generate a factory for all messages inside the current compilation unit
        TypeSpec messageFactory = messageFactoryGenerator.generateMessageFactory(annotatedMessages);

        writeToFile(messageGroup.packageName(), messageFactory);
    }

    /**
     * Generates the following classes for the current compilation unit.
     *
     * <ol>
     *     <li>{@link MessageSerializer};</li>
     *     <li>{@link MessageDeserializer};</li>
     *     <li>{@link MessageSerializationFactory};</li>
     *     <li>Helper class for adding all generated serialization factories to a
     *     {@link MessageSerializationRegistry}.</li>
     * </ol>
     */
    private void generateSerializers(Collection<MessageClass> annotatedMessages, MessageGroupWrapper messageGroup) {
        List<MessageClass> serializableMessages = annotatedMessages.stream()
                .filter(MessageClass::isAutoSerializable)
                .collect(toList());

        if (serializableMessages.isEmpty()) {
            return;
        }

        var factories = new HashMap<MessageClass, TypeSpec>();

        var serializerGenerator = new MessageSerializerGenerator(processingEnv, messageGroup);
        var deserializerGenerator = new MessageDeserializerGenerator(processingEnv, messageGroup);
        var factoryGenerator = new SerializationFactoryGenerator(processingEnv, messageGroup);
        var initializerGenerator = new RegistryInitializerGenerator(processingEnv, messageGroup);

        for (MessageClass message : serializableMessages) {
            try {
                // MessageSerializer
                TypeSpec serializer = serializerGenerator.generateSerializer(message);

                writeToFile(message.packageName(), serializer);

                // MessageDeserializer
                TypeSpec deserializer = deserializerGenerator.generateDeserializer(message);

                writeToFile(message.packageName(), deserializer);

                // MessageSerializationFactory
                TypeSpec factory = factoryGenerator.generateFactory(message, serializer, deserializer);

                writeToFile(message.packageName(), factory);

                factories.put(message, factory);
            } catch (ProcessingException e) {
                throw new ProcessingException(e.getMessage(), e.getCause(), message.element());
            }
        }

        TypeSpec registryInitializer = initializerGenerator.generateRegistryInitializer(factories);

        writeToFile(messageGroup.packageName(), registryInitializer);
        writeServiceFile(messageGroup.packageName(), registryInitializer);
    }

    /**
     * Validates the annotated messages.
     *
     * <ol>
     *     <li>{@link Transferable} annotation is present on a valid element;</li>
     *     <li>No messages with the same message type exist.</li>
     * </ol>
     */
    private void validateMessages(Collection<MessageClass> messages) {
        var typeUtils = new TypeUtils(processingEnv);

        var messageTypesSet = new HashSet<Short>();

        for (MessageClass message : messages) {
            TypeElement element = message.element();

            boolean isValid = element.getKind() == ElementKind.INTERFACE
                    && typeUtils.hasSuperInterface(element, NetworkMessage.class);

            if (!isValid) {
                var errorMsg = String.format(
                        "%s annotation must only be present on interfaces that extend %s",
                        Transferable.class, NetworkMessage.class
                );

                throw new ProcessingException(errorMsg, null, element);
            }

            short messageType = message.messageType();

            if (!messageTypesSet.add(messageType)) {
                var errorMsg = String.format(
                        "Conflicting message types in a group, message with type %d already exists",
                        messageType
                );

                throw new ProcessingException(errorMsg, null, element);
            }
        }
    }

    /**
     * Extracts and validates the declared message group types marked with the {@link MessageGroup} annotation.
     */
    private MessageGroupWrapper getMessageGroup(RoundEnvironment roundEnv, @Nullable IncrementalCompilationConfig config) {
        Set<? extends Element> messageGroupSet = roundEnv.getElementsAnnotatedWith(MessageGroup.class);

        Elements elementUtils = processingEnv.getElementUtils();

        if (messageGroupSet.isEmpty()) {
            if (config != null) {
                TypeElement typeElement = elementUtils.getTypeElement(config.messageGroupClassName().canonicalName());
                if (typeElement != null) {
                    return new MessageGroupWrapper(typeElement);
                }
            }

            Set<String> packageNames = roundEnv.getRootElements().stream()
                    .map(elementUtils::getPackageOf)
                    .map(PackageElement::getQualifiedName)
                    .map(Name::toString)
                    .collect(toSet());

            throw new ProcessingException(String.format(
                    "No message groups (classes annotated with @%s) found while processing messages from the following packages: %s",
                    MessageGroup.class.getSimpleName(),
                    packageNames
            ));
        }

        if (messageGroupSet.size() != 1) {
            List<String> sortedNames = messageGroupSet.stream()
                    .map(Object::toString)
                    .sorted()
                    .collect(toList());

            throw new ProcessingException(String.format(
                    "Invalid number of message groups (classes annotated with @%s), only one can be present in a compilation unit: %s",
                    MessageGroup.class.getSimpleName(),
                    sortedNames
            ));
        }

        TypeElement groupElement = (TypeElement) messageGroupSet.iterator().next();

        return new MessageGroupWrapper(groupElement);
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

    /**
     * Creates a Java SPI file for the {@link MessageSerializationRegistryInitializer} implementation in a module.
     */
    private void writeServiceFile(String packageName, TypeSpec registryInitializer) {
        try {
            FileObject resource = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "", SPI_FILE_NAME);

            try (var writer = new BufferedWriter(resource.openWriter())) {
                writer.write(packageName);
                writer.write('.');
                writer.write(registryInitializer.name);
            }
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage(), e);
        }
    }

    /** Generates utility classes for {@link Enum}s in network messages. */
    private void generateEnumTransferableUtils(Collection<MessageClass> messageClasses, MessageGroupWrapper messageGroup) {
        List<MessageClass> serializableMessages = collectAutoSerializableMessageClasses(messageClasses);

        // If messages are not automatically serializable/deserializable, then there is no need for utility classes for enum for them.
        if (serializableMessages.isEmpty()) {
            return;
        }

        var enumMethodsGenerator = new EnumMethodsGenerator(processingEnv);

        for (TypeMirror enumType : enumMethodsGenerator.collectEnums(messageClasses)) {
            TypeSpec enumTransferableUtils = enumMethodsGenerator.generateEnumTransferableUtils(enumType);

            writeToFile(messageGroup.packageName(), enumTransferableUtils);
        }
    }

    private static List<MessageClass> collectAutoSerializableMessageClasses(Collection<MessageClass> messageClasses) {
        return messageClasses.stream()
                .filter(MessageClass::isAutoSerializable)
                .collect(toList());
    }
}
