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

package org.apache.ignite.internal.error.code.generators;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

/**
 * Generic base class for error codes generators.
 */
public abstract class GenericGenerator implements AbstractCodeGenerator {
    final ProcessingEnvironment processingEnvironment;
    final Path outFilePath;
    BufferedWriter writer;
    static final int groupShift = 16;

    protected void line(String str) throws IOException {
        writer.write(str);
        writer.newLine();
    }

    protected void line() throws IOException {
        line("");
    }

    GenericGenerator(ProcessingEnvironment processingEnvironment, Path outFilePath) {
        this.processingEnvironment = processingEnvironment;
        this.outFilePath = outFilePath;
    }
}
