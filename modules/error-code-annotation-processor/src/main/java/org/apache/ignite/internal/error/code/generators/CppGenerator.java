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

import com.google.common.base.CaseFormat;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupDescriptor;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupProcessorException;

/**
 * C++ generator for Error Codes.
 */
public class CppGenerator extends GenericGenerator {
    private static final String SuffixToChop = "_ERR";

    public CppGenerator(ProcessingEnvironment processingEnvironment, Path outFilePath) {
        super(processingEnvironment, outFilePath);
    }

    private void generateHeader() throws IOException {
        line("#pragma once");
        line();
        line("#include <cstdint>");
        line();
    }

    private void generateEnum(String comment, String name, List<String> valueNames, List<Integer> values) throws IOException {
        if (valueNames.size() != values.size()) {
            throw new ErrorCodeGroupProcessorException("valueNames.size() != values.size()");
        }

        line(comment);
        line("enum class " + name + " : underlying_t {");
        for (int i = 0; i < valueNames.size(); i++) {
            var last = i == valueNames.size() - 1;
            line(String.format("    %s = 0x%s%s", valueNames.get(i), Integer.toHexString(values.get(i)), last ? "" : ","));
        }
        line("};");
        line();
    }

    private void generateErrorCodeGroup(ErrorCodeGroupDescriptor descriptor) throws IOException {
        var groupCode = descriptor.groupCode;
        generateEnum("// " + descriptor.className + " group.",
                CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, descriptor.className),
                descriptor.errorCodes.stream().map(ec -> composeName(ec.name)).collect(Collectors.toList()),
                descriptor.errorCodes.stream().map(ec -> composeCode(groupCode, ec.code)).collect(Collectors.toList()));
    }

    private void generateNamespace(List<ErrorCodeGroupDescriptor> descriptors) throws IOException {
        generateHeader();
        line("namespace ignite {");
        line();
        line("namespace error_codes {");
        line();
        line("using underlying_t = std::uint32_t;");
        line();
        line("static inline const std::uint32_t group_shift = " + groupShift + ";");
        line();
        generateEnum("// Error groups codes.",
                "group",
                descriptors.stream().map(d -> d.groupName).collect(Collectors.toList()),
                descriptors.stream().map(d -> d.groupCode).collect(Collectors.toList()));
        line("inline group get_group_by_error_code(const underlying_t code) {");
        line("    return group(code >> group_shift);");
        line("}");
        line();
        for (var descriptor : descriptors) {
            generateErrorCodeGroup(descriptor);
        }
        line("} // namespace error_codes");
        line();
        line("} // namespace ignite");
    }

    private static int composeCode(int groupCode, int errorCode) {
        return groupCode << groupShift | errorCode;
    }

    private static String composeName(String name) {
        if (name.endsWith(SuffixToChop)) {
            return name.substring(0, name.length() - SuffixToChop.length());
        }

        return name;
    }

    @Override
    public void generate(List<ErrorCodeGroupDescriptor> descriptors) {
        try {
            FileObject resource = processingEnvironment.getFiler().createResource(StandardLocation.SOURCE_OUTPUT,
                    "",
                    outFilePath.toString());

            writer = new BufferedWriter(resource.openWriter());
            generateNamespace(descriptors);
            writer.flush();
        } catch (IOException e) {
            throw new ErrorCodeGroupProcessorException("IO exception during annotation processing", e);
        }
    }
}
