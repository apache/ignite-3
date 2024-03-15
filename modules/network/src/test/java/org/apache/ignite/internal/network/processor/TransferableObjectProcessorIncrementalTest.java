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

import static com.squareup.javapoet.ClassName.get;
import static org.apache.ignite.internal.network.processor.InMemoryJavaFileManager.uriForFileObject;
import static org.apache.ignite.internal.network.processor.InMemoryJavaFileManager.uriForJavaFileObject;
import static org.apache.ignite.internal.network.processor.IncrementalCompilationConfig.CONFIG_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.testing.compile.JavaFileObjects;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for the {@link TransferableObjectProcessor} incremental compilation.
 */
@ExtendWith(MockitoExtension.class)
public class TransferableObjectProcessorIncrementalTest extends BaseIgniteAbstractTest {
    /**
     * Package name of the test sources.
     */
    private static final String RESOURCE_PACKAGE_NAME = "org.apache.ignite.internal.network.processor";

    /** File manager for incremental compilation. */
    private InMemoryJavaFileManager fileManager;

    /** Javac diagnostic collector. */
    private final DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<>();

    @Mock
    private ProcessingEnvironment env;

    @Mock
    private Filer filer;

    @BeforeEach
    void setUp() {
        when(env.getFiler()).thenReturn(filer);

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
        compilationObjects1.add(createTransferable(testMessageClass, 0));

        Map<URI, JavaFileObject> compilation1 = compile(compilationObjects1);

        IncrementalCompilationConfig cfg1 = readConfig(compilation1);

        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageGroup), cfg1.messageGroupClassName());
        assertEquals(1, cfg1.messageClasses().size());
        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageClass), cfg1.messageClasses().get(0));

        URI factoryUri = uriForJavaFileObject(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName + "Factory",
                Kind.SOURCE
        );

        String messageFactory1 = readJavaFileObject(compilation1.get(factoryUri));

        assertTrue(messageFactory1.contains("TestMessageImpl.builder();"));

        List<JavaFileObject> compilationObjects2 = new ArrayList<>();
        compilationObjects2.add(createNonTransferable(testMessageClass));
        compilationObjects2.add(createTransferable(testMessageClass2, 0));

        Map<URI, JavaFileObject> compilation2 = compile(compilationObjects2);

        IncrementalCompilationConfig cfg2 = readConfig(compilation2);

        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageGroup), cfg2.messageGroupClassName());
        assertEquals(1, cfg2.messageClasses().size());
        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageClass2), cfg2.messageClasses().get(0));

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
        compilationObjects1.add(createTransferable(testMessageClass, 0));

        Map<URI, JavaFileObject> compilation1 = compile(compilationObjects1);

        IncrementalCompilationConfig cfg1 = readConfig(compilation1);

        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageGroup), cfg1.messageGroupClassName());
        assertEquals(1, cfg1.messageClasses().size());
        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageClass), cfg1.messageClasses().get(0));

        URI factoryUri = uriForJavaFileObject(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName + "Factory",
                Kind.SOURCE
        );

        String messageFactory1 = readJavaFileObject(compilation1.get(factoryUri));

        assertTrue(messageFactory1.contains("TestMessageImpl.builder();"));

        List<JavaFileObject> compilationObjects2 = new ArrayList<>();
        compilationObjects2.add(createTransferable(testMessageClass2, 1));

        Map<URI, JavaFileObject> compilation2 = compile(compilationObjects2);

        IncrementalCompilationConfig cfg2 = readConfig(compilation2);

        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageGroup), cfg2.messageGroupClassName());
        assertEquals(2, cfg2.messageClasses().size());
        assertEquals(
                Set.of(get(RESOURCE_PACKAGE_NAME, testMessageClass), get(RESOURCE_PACKAGE_NAME, testMessageClass2)),
                new HashSet<>(cfg2.messageClasses())
        );

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
        compilationObjects1.add(createTransferable(testMessageClass, 0));

        Map<URI, JavaFileObject> compilation1 = compile(compilationObjects1);

        IncrementalCompilationConfig cfg1 = readConfig(compilation1);

        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageGroup), cfg1.messageGroupClassName());
        assertEquals(1, cfg1.messageClasses().size());
        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageClass), cfg1.messageClasses().get(0));

        URI factory1Uri = uriForJavaFileObject(
                StandardLocation.SOURCE_OUTPUT,
                RESOURCE_PACKAGE_NAME + "." + testMessageGroupName + "Factory",
                Kind.SOURCE
        );

        String messageFactory1 = readJavaFileObject(compilation1.get(factory1Uri));

        assertTrue(messageFactory1.contains("TestMessageImpl.builder();"));

        List<JavaFileObject> compilationObjects2 = new ArrayList<>();
        compilationObjects2.add(createMessageGroup(testMessageGroup2, testMessageGroupName2));
        compilationObjects2.add(createTransferable(testMessageClass, 0));

        Map<URI, JavaFileObject> compilation2 = compile(compilationObjects2);

        IncrementalCompilationConfig cfg2 = readConfig(compilation2);

        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageGroup2), cfg2.messageGroupClassName());
        assertEquals(1, cfg2.messageClasses().size());
        assertEquals(get(RESOURCE_PACKAGE_NAME, testMessageClass), cfg2.messageClasses().get(0));

        URI factory2Uri = uriForJavaFileObject(
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
                builder.append(line).append('\n');
            }

            return builder.toString();
        }
    }

    private JavaFileObject createTransferable(String className, int msgId) {
        @Language("JAVA") String code =
                "package " + RESOURCE_PACKAGE_NAME + ";\n"
                        + "import org.apache.ignite.internal.network.NetworkMessage;\n"
                        + "import org.apache.ignite.internal.network.annotations.Transferable;\n"
                        + "\n"
                        + "\n"
                        + "@Transferable(value = " + msgId + ")\n"
                        + "public interface " + className + " extends NetworkMessage {\n"
                        + "    String foo();\n"
                        + "}\n";
        return JavaFileObjects.forSourceString(className, code);
    }

    private JavaFileObject createMessageGroup(String className, String groupName) {
        @Language("JAVA") String code =
                "package " + RESOURCE_PACKAGE_NAME + ";\n"
                        + "\n"
                        + "    import org.apache.ignite.internal.network.annotations.MessageGroup;\n"
                        + "\n"
                        + "@MessageGroup(groupType = 1, groupName = \"" + groupName + "\")\n"
                        + "public class " + className + " {\n"
                        + "}\n";
        return JavaFileObjects.forSourceString(className, code);
    }

    private JavaFileObject createNonTransferable(String className) {
        @Language("JAVA") String code =
                "package " + RESOURCE_PACKAGE_NAME + ";\n"
                        + "import org.apache.ignite.internal.network.NetworkMessage;\n"
                        + "\n"
                        + "\n"
                        + "public interface " + className + " extends NetworkMessage {\n"
                        + "    String foo();\n"
                        + "}\n";
        return JavaFileObjects.forSourceString(className, code);
    }

    private Map<URI, JavaFileObject> compile(Iterable<? extends JavaFileObject> files) {
        JavaCompiler systemJavaCompiler = ToolProvider.getSystemJavaCompiler();

        CompilationTask task = systemJavaCompiler
                .getTask(null, fileManager, diagnosticCollector, Collections.emptyList(), Set.of(), files);

        task.setProcessors(Collections.singleton(new TransferableObjectProcessor()));

        Boolean result = task.call();

        assertTrue(result);

        return fileManager.getOutputFiles();
    }

    private IncrementalCompilationConfig readConfig(Map<URI, JavaFileObject> compilation) throws IOException {
        when(filer.getResource(StandardLocation.CLASS_OUTPUT, "", CONFIG_FILE_NAME))
                .thenReturn(compilation.get(uriForMessagesFile()));

        return IncrementalCompilationConfig.readConfig(env);
    }

    private static URI uriForMessagesFile() {
        return uriForFileObject(StandardLocation.CLASS_OUTPUT, "", CONFIG_FILE_NAME);
    }
}
