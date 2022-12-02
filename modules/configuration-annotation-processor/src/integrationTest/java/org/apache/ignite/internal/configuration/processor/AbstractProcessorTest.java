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

package org.apache.ignite.internal.configuration.processor;

import static com.google.testing.compile.Compiler.javac;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getChangeName;
import static org.apache.ignite.internal.configuration.processor.ConfigurationProcessorUtils.getViewName;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import com.squareup.javapoet.ClassName;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.tools.JavaFileObject;

/**
 * Base class for configuration annotation processor tests.
 */
public class AbstractProcessorTest {
    /**
     * Compiles a given set of source files.
     */
    protected static Compilation compile(ClassName... schemaClasses) {
        List<String> fileNames = Arrays.stream(schemaClasses)
                .map(name -> {
                    String folderName = name.packageName().replace(".", "/");

                    return String.format("%s/%s.java", folderName, name.simpleName());
                })
                .collect(Collectors.toList());

        List<JavaFileObject> fileObjects = fileNames.stream().map(JavaFileObjects::forResource).collect(Collectors.toList());

        return javac().withProcessors(new ConfigurationProcessor()).compile(fileObjects);
    }

    /**
     * Compile set of classes.
     *
     * @param schemaClasses Configuration schema classes.
     * @return Result of batch compilation.
     */
    protected static BatchCompilation batchCompile(ClassName... schemaClasses) {
        return new BatchCompilation(Arrays.asList(schemaClasses), compile(schemaClasses));
    }

    /**
     * Get {@link ConfigSet} object from generated classes.
     *
     * @param clazz            Configuration schema ClassName.
     * @param generatedClasses Map with all generated classes.
     * @return ConfigSet.
     */
    protected static ConfigSet getConfigSet(ClassName clazz, final Map<ClassName, JavaFileObject> generatedClasses) {
        final ClassName viewName = getViewName(clazz);
        final ClassName changeName = getChangeName(clazz);

        final JavaFileObject viewClass = generatedClasses.get(viewName);
        final JavaFileObject changeClass = generatedClasses.get(changeName);

        return new ConfigSet(viewClass, changeClass);
    }

    /**
     * Get {@link ClassName} object from generated file path.
     *
     * @param fileName File path.
     * @return ClassName.
     */
    protected static ClassName fromGeneratedFilePath(String fileName) {
        final String filePath = fileName.replace("/SOURCE_OUTPUT/", "")
                .replace("/CLASS_OUTPUT/", "");
        return fromFilePath(filePath);
    }

    /**
     * Get {@link ClassName} object from file path.
     *
     * @param fileName File path.
     * @return ClassName.
     */
    protected static ClassName fromFilePath(String fileName) {
        int slashIdx = fileName.lastIndexOf("/");
        int dotExtIdx = fileName.lastIndexOf(".");

        String packageName = fileName.substring(0, slashIdx).replace("/", ".");

        final String className = fileName.substring(slashIdx + 1, dotExtIdx);

        return ClassName.get(packageName, className);
    }

    /**
     * Result of multiple compiled schema classes.
     */
    protected static class BatchCompilation {
        /** Generated source files. */
        private final List<JavaFileObject> generatedSources;

        /** Generated source files mapped by config schema class name. */
        private final Map<ClassName, JavaFileObject> generatedSourcesMap;

        /** Config class sets by config schema class name. */
        private final Map<ClassName, ConfigSet> classSets;

        /** Generated classes mapped by config schema class name. */
        private final Map<ClassName, JavaFileObject> generatedClassesMap;

        /** Compilation status. */
        private final Compilation compilationStatus;

        /**
         * Constructor.
         *
         * @param schemaClasses List of schema class names.
         * @param compilation   Compilation status.
         */
        public BatchCompilation(List<ClassName> schemaClasses, Compilation compilation) {
            this.compilationStatus = compilation;

            generatedSources = compilation.generatedSourceFiles();

            generatedSourcesMap = generatedSources.stream()
                    .collect(toMap(object -> fromGeneratedFilePath(object.getName()), Function.identity()));

            classSets = schemaClasses.stream().collect(
                    toMap(name -> name, name -> getConfigSet(name, generatedSourcesMap))
            );

            generatedClassesMap = compilation.generatedFiles().stream()
                    .filter(generatedFile -> generatedFile.getKind().equals(JavaFileObject.Kind.CLASS))
                    .collect(toMap(object -> fromGeneratedFilePath(object.getName()), Function.identity()));
        }

        /**
         * Get config class set by schema class name.
         *
         * @param schemaClass Schema class name.
         * @return Config class set.
         */
        public ConfigSet getBySchema(ClassName schemaClass) {
            return classSets.get(schemaClass);
        }

        /**
         * Get compilation status.
         *
         * @return Compilation status.
         */
        public Compilation getCompilationStatus() {
            return compilationStatus;
        }

        /**
         * Get all generated source files.
         *
         * @return Generated source files.
         */
        public List<JavaFileObject> generatedSources() {
            return generatedSources;
        }

        /**
         * Returns all generated class files.
         */
        public Map<ClassName, JavaFileObject> generatedClassesMap() {
            return Map.copyOf(generatedClassesMap);
        }
    }
}
