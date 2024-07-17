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

import com.squareup.javapoet.ClassName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import org.jetbrains.annotations.Nullable;

/**
 * Incremental configuration of the {@link TransferableObjectProcessor}.
 * Holds data between (re-)compilations.
 * <br>
 * The serialized format of this config is as follows:
 * <br>
 * First line: message group class' name
 * <br>
 * Next lines: message class' names
 * <br>
 * Every class name is written as {@code packageName + " " + simpleName1 + " " + ... + simpleNameN}, e.g.
 * "org.apache.ignite OuterClass InnerClass EvenMoreInnerClass".
 */
class IncrementalCompilationConfig {
    /** Incremental compilation configuration file name. */
    static final String CONFIG_FILE_NAME = "META-INF/transferable.messages";

    /** Message group class name. */
    private final ClassName messageGroupClassName;

    /** Messages. */
    private final List<ClassName> messageClasses;

    IncrementalCompilationConfig(ClassName messageGroupClassName, List<ClassName> messageClasses) {
        this.messageGroupClassName = messageGroupClassName;
        this.messageClasses = List.copyOf(messageClasses);
    }

    /**
     * Saves configuration on disk.
     *
     * @param processingEnv Processing environment.
     */
    void writeConfig(ProcessingEnvironment processingEnv) {
        Filer filer = processingEnv.getFiler();

        FileObject fileObject;
        try {
            fileObject = filer.createResource(StandardLocation.CLASS_OUTPUT, "", CONFIG_FILE_NAME);
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage(), e);
        }

        try (BufferedWriter writer = new BufferedWriter(fileObject.openWriter())) {
            writeClassName(writer, messageGroupClassName);

            for (ClassName messageClassName : messageClasses) {
                writeClassName(writer, messageClassName);
            }
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage(), e);
        }
    }

    /**
     * Reads configuration from disk.
     *
     * @param processingEnv Processing environment.
     */
    @Nullable
    static IncrementalCompilationConfig readConfig(ProcessingEnvironment processingEnv) {
        Filer filer = processingEnv.getFiler();

        FileObject resource;

        try {
            resource = filer.getResource(StandardLocation.CLASS_OUTPUT, "", CONFIG_FILE_NAME);
        } catch (IOException e) {
            return null;
        }

        try (BufferedReader bufferedReader = new BufferedReader(resource.openReader(true))) {
            String messageClassNameString = bufferedReader.readLine();

            if (messageClassNameString == null) {
                return null;
            }

            ClassName messageClassName = readClassName(messageClassNameString);

            List<ClassName> messages = bufferedReader.lines().map(IncrementalCompilationConfig::readClassName).collect(toList());

            return new IncrementalCompilationConfig(messageClassName, messages);
        } catch (FileNotFoundException | NoSuchFileException e) {
            // PathFileObject throws this internally although this exception is not declared in the openReader javadoc, so
            // this is the only way to know if the file doesn't exist.
            return null;
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage(), e);
        }
    }

    /**
     * Writes class name with all the enclosing classes.
     *
     * @param writer Writer.
     * @param className Class name.
     * @throws IOException If failed.
     */
    private static void writeClassName(BufferedWriter writer, ClassName className) throws IOException {
        writer.write(className.packageName());
        writer.write(' ');

        for (String enclosingSimpleName : className.simpleNames()) {
            writer.write(enclosingSimpleName);
            writer.write(' ');
        }

        writer.newLine();
    }

    /**
     * Reads class name.
     *
     * @param line Line.
     * @return Class name.
     */
    private static ClassName readClassName(String line) {
        String[] split = line.split(" ");

        String packageName = split[0];

        String firstSimpleName = split[1];

        String[] simpleNames = split.length > 2 ? Arrays.copyOfRange(split, 2, split.length) : new String[0];

        return ClassName.get(packageName, firstSimpleName, simpleNames);
    }

    ClassName messageGroupClassName() {
        return messageGroupClassName;
    }

    List<ClassName> messageClasses() {
        return messageClasses;
    }
}
