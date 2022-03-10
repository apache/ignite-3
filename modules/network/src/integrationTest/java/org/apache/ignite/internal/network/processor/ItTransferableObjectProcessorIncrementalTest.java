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

package org.apache.ignite.internal.network.processor;

import static org.apache.ignite.internal.network.processor.IncrementalCompilationConfig.CONFIG_FILE_NAME;
import static org.apache.ignite.internal.network.processor.IncrementalCompilationConfig.readClassName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.testing.compile.JavaFileObjects;
import com.squareup.javapoet.ClassName;
import java.io.BufferedReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager.Location;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@link TransferableObjectProcessor} incremental compilation.
 */
public class ItTransferableObjectProcessorIncrementalTest {
    /**
     * Package name of the test sources.
     */
    private static final String RESOURCE_PACKAGE_NAME = "org.apache.ignite.internal.network.processor";

    /** File manager for incremental compilation. */
    private InMemoryJavaFileManager fileManager;

    @BeforeEach
    void setUp() {
        DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<>();
        JavaCompiler systemJavaCompiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager standardFileManager = systemJavaCompiler.getStandardFileManager(diagnosticCollector, Locale.getDefault(),
                StandardCharsets.UTF_8);

        this.fileManager = new InMemoryJavaFileManager(standardFileManager);
    }

    @Test
    public void testIncrementalRemoveTransferable() throws Exception {
        String testMessageGroup = "MsgGroup";
        String testMessageGroupName = "GroupName";
        String testMessageClass = "TestMessage";
        String testMessageClass2 = "SomeMessage";

        var compilationObjects1 = new ArrayList<JavaFileObject>();
        JavaFileObject messageGroupObject = createMessageGroup(testMessageGroup, testMessageGroupName);
        compilationObjects1.add(messageGroupObject);
        compilationObjects1.add(createTransferable(testMessageClass));

        Map<URI, JavaFileObject> compilation1 = compile(compilationObjects1);

        JavaFileObject messageRegistry1 = compilation1.get(uriForMessagesFile());
        try (BufferedReader bufferedReader = new BufferedReader(messageRegistry1.openReader(true))) {
            ClassName messageGroupClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageGroup), messageGroupClass);

            ClassName messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass), messageClass);

            assertNull(bufferedReader.readLine());
        }

        URI factoryUri = uriForJavaClass(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName + "Factory",
                Kind.SOURCE
        );

        String messageFactory1 = readJavaFileObject(compilation1.get(factoryUri));

        assertTrue(messageFactory1.contains("TestMessageImpl.builder();"));

        List<JavaFileObject> compilationObjects2 = new ArrayList<>();
        compilationObjects2.add(createNonTransferable(testMessageClass));
        compilationObjects2.add(createTransferable(testMessageClass2));

        Map<URI, JavaFileObject> compilation2 = compile(compilationObjects2);

        JavaFileObject messageRegistry2 = compilation2.get(uriForMessagesFile());
        try (BufferedReader bufferedReader = new BufferedReader(messageRegistry2.openReader(true))) {
            ClassName messageGroupClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageGroup), messageGroupClass);

            ClassName messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass2), messageClass);

            assertNull(bufferedReader.readLine());
        }

        String messageFactory2 = readJavaFileObject(compilation2.get(factoryUri));

        assertFalse(messageFactory2.contains("TestMessageImpl.builder();"));
        assertTrue(messageFactory2.contains("SomeMessageImpl.builder();"));
    }

    @Test
    public void testIncrementalAddTransferable() throws Exception {
        String testMessageGroup = "MsgGroup";
        String testMessageGroupName = "GroupName";
        String testMessageClass = "TestMessage";
        String testMessageClass2 = "SomeMessage";

        var compilationObjects1 = new ArrayList<JavaFileObject>();
        JavaFileObject messageGroupObject = createMessageGroup(testMessageGroup, testMessageGroupName);
        compilationObjects1.add(messageGroupObject);
        compilationObjects1.add(createTransferable(testMessageClass));

        Map<URI, JavaFileObject> compilation1 = compile(compilationObjects1);

        JavaFileObject messageRegistry1 = compilation1.get(uriForMessagesFile());
        try (BufferedReader bufferedReader = new BufferedReader(messageRegistry1.openReader(true))) {
            ClassName messageGroupClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageGroup), messageGroupClass);

            ClassName messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass), messageClass);

            assertNull(bufferedReader.readLine());
        }

        URI factoryUri = uriForJavaClass(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName + "Factory",
                Kind.SOURCE
        );

        String messageFactory1 = readJavaFileObject(compilation1.get(factoryUri));

        assertTrue(messageFactory1.contains("TestMessageImpl.builder();"));

        List<JavaFileObject> compilationObjects2 = new ArrayList<>();
        compilationObjects2.add(createTransferable(testMessageClass2));

        Map<URI, JavaFileObject> compilation2 = compile(compilationObjects2);

        JavaFileObject messageRegistry2 = compilation2.get(uriForMessagesFile());
        try (BufferedReader bufferedReader = new BufferedReader(messageRegistry2.openReader(true))) {
            ClassName messageGroupClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageGroup), messageGroupClass);

            ClassName messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass), messageClass);

            messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass2), messageClass);

            assertNull(bufferedReader.readLine());
        }

        String messageFactory2 = readJavaFileObject(compilation2.get(factoryUri));

        assertTrue(messageFactory2.contains("TestMessageImpl.builder();"));
        assertTrue(messageFactory2.contains("SomeMessageImpl.builder();"));
    }

    @Test
    public void testChangeMessageGroup() throws Exception {
        String testMessageGroup = "MsgGroup";
        String testMessageGroupName = "GroupName";
        String testMessageGroup2 = "MyNewGroup";
        String testMessageGroupName2 = "NewGroupName";
        String testMessageClass = "TestMessage";

        var compilationObjects1 = new ArrayList<JavaFileObject>();
        compilationObjects1.add(createMessageGroup(testMessageGroup, testMessageGroupName));
        compilationObjects1.add(createTransferable(testMessageClass));

        Map<URI, JavaFileObject> compilation1 = compile(compilationObjects1);

        JavaFileObject messageRegistry1 = compilation1.get(uriForMessagesFile());
        try (BufferedReader bufferedReader = new BufferedReader(messageRegistry1.openReader(true))) {
            ClassName messageGroupClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageGroup), messageGroupClass);

            ClassName messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass), messageClass);

            assertNull(bufferedReader.readLine());
        }

        URI factory1Uri = uriForJavaClass(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName + "Factory",
                Kind.SOURCE
        );

        String messageFactory1 = readJavaFileObject(compilation1.get(factory1Uri));

        assertTrue(messageFactory1.contains("TestMessageImpl.builder();"));

        List<JavaFileObject> compilationObjects2 = new ArrayList<>();
        compilationObjects2.add(createMessageGroup(testMessageGroup2, testMessageGroupName2));
        compilationObjects2.add(createTransferable(testMessageClass));

        Map<URI, JavaFileObject> compilation2 = compile(compilationObjects2);

        JavaFileObject messageRegistry2 = compilation2.get(uriForMessagesFile());
        try (BufferedReader bufferedReader = new BufferedReader(messageRegistry2.openReader(true))) {
            ClassName messageGroupClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageGroup2), messageGroupClass);

            ClassName messageClass = readClassName(bufferedReader.readLine());

            assertEquals(ClassName.get(RESOURCE_PACKAGE_NAME, testMessageClass), messageClass);

            assertNull(bufferedReader.readLine());
        }

        URI factory2Uri = uriForJavaClass(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName2 + "Factory",
                Kind.SOURCE
        );

        String messageFactory2 = readJavaFileObject(compilation2.get(factory2Uri));

        assertTrue(messageFactory2.contains("TestMessageImpl.builder();"));
    }

    private String readJavaFileObject(JavaFileObject object) throws Exception {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(object.openReader(true))) {
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                builder.append(line).append("\n");
            }

            return builder.toString();
        }
    }

    private JavaFileObject createTransferable(String className) {
        @Language("JAVA") String code =
                "package " + RESOURCE_PACKAGE_NAME + ";\n"
                  + "import org.apache.ignite.network.NetworkMessage;\n"
                  + "import org.apache.ignite.network.annotations.Transferable;\n"
                    + "\n"
                + "\n"
                + "@Transferable(value = 0)\n"
                + "public interface " + className + " extends NetworkMessage {\n"
                    + "    String foo();\n"
                    + "}\n";
        return JavaFileObjects.forSourceString(className, code);
    }

    private JavaFileObject createMessageGroup(String className, String groupName) {
        @Language("JAVA") String code =
                "package " + RESOURCE_PACKAGE_NAME + ";\n"
                    + "\n"
                    + "    import org.apache.ignite.network.annotations.MessageGroup;\n"
                    + "\n"
                    + "@MessageGroup(groupType = 1, groupName = \"" + groupName + "\")\n"
                    + "public class " + className + " {\n"
                    + "}\n";
        return JavaFileObjects.forSourceString(className, code);
    }

    private JavaFileObject createNonTransferable(String className) {
        @Language("JAVA") String code =
            "package " + RESOURCE_PACKAGE_NAME + ";\n"
                + "import org.apache.ignite.network.NetworkMessage;\n"
                + "\n"
                + "\n"
                + "public interface " + className + " extends NetworkMessage {\n"
                + "    String foo();\n"
                + "}\n";
        return JavaFileObjects.forSourceString(className, code);
    }

    private Map<URI, JavaFileObject> compile(Iterable<? extends JavaFileObject> files) {
        JavaCompiler systemJavaCompiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnosticListener = new DiagnosticCollector<>();
        CompilationTask task = systemJavaCompiler
                .getTask(null, fileManager, diagnosticListener, Collections.emptyList(), Set.of(), files);

        task.setProcessors(Collections.singleton(new TransferableObjectProcessor()));

        Boolean result = task.call();

        assertTrue(result);

        Iterable<JavaFileObject> generatedFiles = fileManager.getOutputFiles();

        return StreamSupport.stream(generatedFiles.spliterator(), false)
                .collect(Collectors.toMap(FileObject::toUri, Function.identity()));
    }


    private static URI uriForMessagesFile() {
        return uriForObject(StandardLocation.CLASS_OUTPUT, CONFIG_FILE_NAME, Kind.OTHER);
    }

    private static URI uriForJavaClass(Location location, String className, Kind kind) {
        return URI.create("mem:///" + location.getName() + '/' + className.replace('.', '/') + kind.extension);
    }

    private static URI uriForObject(Location location, String fileName, Kind kind) {
        return URI.create("mem:///" + location.getName() + '/' + fileName + kind.extension);
    }
}
