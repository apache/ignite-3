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
package org.apache.ignite.configuration.processor.internal;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.CompilationSubject;
import com.google.testing.compile.JavaFileObjects;
import com.squareup.javapoet.ClassName;
import java.util.Map;
import java.util.stream.Collectors;
import javax.tools.JavaFileObject;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static com.google.testing.compile.Compiler.javac;
import static org.apache.ignite.configuration.processor.internal.HasFieldMatcher.hasFields;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessorTest {

    @Test
    public void test() throws Exception {
        final String fileName = "org/apache/ignite/configuration/processor/internal/TestConfigurationSchema.java";

        final ClassName clazz = fromFilePath(fileName);

        final Compilation compilation = javac()
            .withProcessors(new Processor())
            .compile(JavaFileObjects.forResource(fileName));

        CompilationSubject.assertThat(compilation).succeeded();

        final ImmutableList<JavaFileObject> generatedSources = compilation.generatedSourceFiles();

        assertEquals(6, generatedSources.size());

        final Map<ClassName, JavaFileObject> generatedClasses = generatedSources.stream()
            .collect(Collectors.toMap(object -> fromGeneratedFilePath(object.getName()), Functions.identity()));

        final ConfigSet classSet = getConfigSet(clazz, generatedClasses);

        assertTrue(classSet.allGenerated());

        MatcherAssert.assertThat(classSet.getViewClass(), hasFields("value1", "java.lang.String"));
    }

    public ConfigSet getConfigSet(ClassName clazz, final Map<ClassName, JavaFileObject> generatedClasses) {
        final ClassName configurationName = Utils.getConfigurationName(clazz);
        final ClassName viewName = Utils.getViewName(clazz);
        final ClassName initName = Utils.getInitName(clazz);
        final ClassName changeName = Utils.getChangeName(clazz);

        final JavaFileObject configurationFileObject = generatedClasses.get(configurationName);
        final JavaFileObject viewClass = generatedClasses.get(viewName);
        final JavaFileObject initClass = generatedClasses.get(initName);
        final JavaFileObject changeClass = generatedClasses.get(changeName);

        return new ConfigSet(configurationFileObject, viewClass, initClass, changeClass);
    }

    ClassName fromGeneratedFilePath(String fileName) {
        return fromFilePath(fileName.replace("/SOURCE_OUTPUT/", ""));
    }

    ClassName fromFilePath(String fileName) {
        int slashIdx = fileName.lastIndexOf("/");
        int dotJavaIdx = fileName.lastIndexOf(".java");

        String packageName = fileName.substring(0, slashIdx).replaceAll("/", ".");

        final String className = fileName.substring(slashIdx + 1, dotJavaIdx);

        return ClassName.get(packageName, className);
    }

}
