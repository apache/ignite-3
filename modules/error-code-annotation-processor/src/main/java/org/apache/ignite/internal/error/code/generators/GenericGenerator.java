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
import java.util.List;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupDescriptor;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupProcessorException;

/**
 * Generic base class for error codes generators.
 */
public abstract class GenericGenerator implements AbstractCodeGenerator {
    final ProcessingEnvironment processingEnvironment;
    final String outFilePath;
    BufferedWriter writer;
    static final int groupShift = 16;

    protected void line(String str) throws IOException {
        writer.write(str);
        writer.newLine();
    }

    protected void line() throws IOException {
        line("");
    }

    GenericGenerator(ProcessingEnvironment processingEnvironment, String outFilePath) {
        this.processingEnvironment = processingEnvironment;
        this.outFilePath = outFilePath;
    }

    void generateFile(List<ErrorCodeGroupDescriptor> descriptors) throws IOException, ErrorCodeGroupProcessorException {
        throw new ErrorCodeGroupProcessorException("generateFile not implemented!");
    }

    void generateLicense() throws IOException {
        line("/*");
        line(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        line(" * contributor license agreements. See the NOTICE file distributed with");
        line(" * this work for additional information regarding copyright ownership.");
        line(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        line(" * (the \"License\"); you may not use this file except in compliance with");
        line(" * the License. You may obtain a copy of the License at");
        line(" *");
        line(" *      http://www.apache.org/licenses/LICENSE-2.0");
        line(" *");
        line(" * Unless required by applicable law or agreed to in writing, software");
        line(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        line(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        line(" * See the License for the specific language governing permissions and");
        line(" * limitations under the License.");
        line(" */");
        line();
    }

    @Override
    public void generate(List<ErrorCodeGroupDescriptor> descriptors) {
        try {
            FileObject resource = processingEnvironment.getFiler().createResource(StandardLocation.NATIVE_HEADER_OUTPUT,
                    "",
                    outFilePath);

            writer = new BufferedWriter(resource.openWriter());
            generateLicense();
            generateFile(descriptors);
            writer.flush();
        } catch (IOException e) {
            throw new ErrorCodeGroupProcessorException("IO exception during annotation processing", e);
        }
    }
}
