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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.squareup.javapoet.ClassName;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

class IncrementalCompilationConfig {
    private static final String CONFIG_FILE_NAME = "META-INF/messages.file";

    private final String messageGroupClassName;

    private final List<ClassName> messageClasses;

    IncrementalCompilationConfig(String messageGroupClassName, List<ClassName> messageClasses) {
        this.messageGroupClassName = messageGroupClassName;
        this.messageClasses = messageClasses;
    }

    void writeConfig(ProcessingEnvironment processingEnv) {
        Filer filer = processingEnv.getFiler();

        FileObject fileObject = null;
        try {
            fileObject = filer.createResource(StandardLocation.CLASS_OUTPUT, "", CONFIG_FILE_NAME);
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage());
        }
        try (OutputStream out = fileObject.openOutputStream()) {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, UTF_8));
            writer.write(messageGroupClassName);
            writer.newLine();
            for (ClassName messageClassName : messageClasses) {
                writer.write(messageClassName.packageName());
                writer.write(' ');

                List<String> enclosingSimpleNames = new ArrayList<>();
                ClassName enclosing = messageClassName;
                while ((enclosing = enclosing.enclosingClassName()) != null) {
                    enclosingSimpleNames.add(enclosing.simpleName());
                }
                Collections.reverse(enclosingSimpleNames);
                for (String enclosingSimpleName : enclosingSimpleNames) {
                    writer.write(enclosingSimpleName);
                    writer.write(' ');
                }

                writer.write(messageClassName.simpleName());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage());
        }
    }

    static IncrementalCompilationConfig readConfig(ProcessingEnvironment processingEnv) {
        Filer filer = processingEnv.getFiler();

        FileObject resource;

        try {
            resource = filer.getResource(StandardLocation.CLASS_OUTPUT, "", CONFIG_FILE_NAME);
        } catch (IOException e) {
            return null;
        }

        try (Reader reader = resource.openReader(true)) {
            BufferedReader bufferedReader = new BufferedReader(reader);
            String messageClassName = bufferedReader.readLine();

            if (messageClassName == null) {
                return null;
            }

            String line;
            List<ClassName> message = new ArrayList<>();
            while ((line = bufferedReader.readLine()) != null) {
                String[] split = line.split(" ");
                String packageName = split[0];
                String simpleName = split[1];
                String[] simpleNames = {};
                if (split.length > 2) {
                    simpleNames = Arrays.copyOfRange(split, 2, split.length);
                }
                message.add(ClassName.get(packageName, simpleName, simpleNames));
            }
            return new IncrementalCompilationConfig(messageClassName, message);
        } catch (FileNotFoundException | NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new ProcessingException(e.getMessage());
        }
    }

    String messageGroupClassName() {
        return messageGroupClassName;
    }

    List<ClassName> messageClasses() {
        return messageClasses;
    }
}
