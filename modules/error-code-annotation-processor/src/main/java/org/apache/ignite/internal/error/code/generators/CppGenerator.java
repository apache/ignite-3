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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupDescriptor;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupDescriptor.DeprecatedAlias;
import org.apache.ignite.internal.error.code.processor.ErrorCodeGroupProcessorException;

/**
 * C++ generator for Error Codes.
 */
public class CppGenerator extends GenericGenerator {
    private static final String SuffixToChop = "_ERR";

    public CppGenerator(ProcessingEnvironment processingEnvironment, String outFilePath) {
        super(processingEnvironment, outFilePath);
    }

    private void generateHeader() throws IOException {
        line("// THIS IS AUTO-GENERATED FILE. DO NOT EDIT.");
        line();
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

    private void generateErrorCodeGroup(ErrorCodeGroupDescriptor descriptor, boolean lastGroup) throws IOException {
        var groupCode = descriptor.groupCode;
        line("    // " + descriptor.className + " group. Group code: " + descriptor.groupCode);

        for (int i = 0; i < descriptor.errorCodes.size(); i++) {
            var lastInGroup = (i == descriptor.errorCodes.size() - 1) && descriptor.deprecatedAliases.isEmpty();
            var ec = descriptor.errorCodes.get(i);
            var name = composeName(ec.name);
            var code = Integer.toHexString(composeCode(groupCode, ec.code));
            line(String.format("    %s = 0x%s%s", name, code, lastGroup && lastInGroup ? "" : ","));
            if (lastInGroup && !lastGroup) {
                line();
            }
        }

        for (int i = 0; i < descriptor.deprecatedAliases.size(); i++) {
            var lastInGroup = i == descriptor.deprecatedAliases.size() - 1;
            DeprecatedAlias deprecatedAlias = descriptor.deprecatedAliases.get(i);
            String composedName = composeName(deprecatedAlias.alias);
            String composedIdentifier = composeName(deprecatedAlias.target);

            line(String.format("    %s [[deprecated(\"%s is deprecated. Use %s instead.\")]] = %s%s", composedName,
                    composedName, composedIdentifier, composedIdentifier, lastGroup && lastInGroup ? "" : ","));
            if (lastInGroup && !lastGroup) {
                line();
            }
        }
    }

    private void generateErrorCodeEnumStart() throws IOException {
        line("// Error codes.");
        line("enum class code : underlying_t {");
    }

    private void generateErrorCodeEnumEnd() throws IOException {
        line("};");
        line();
    }

    @Override
    void generateFile(List<ErrorCodeGroupDescriptor> descriptors) throws IOException {
        generateHeader();
        line("namespace ignite {");
        line();
        line("namespace error {");
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
        generateErrorCodeEnumStart();
        for (int i = 0; i < descriptors.size(); i++) {
            generateErrorCodeGroup(descriptors.get(i), i == descriptors.size() - 1);
        }
        generateErrorCodeEnumEnd();
        line("} // namespace error");
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
}
