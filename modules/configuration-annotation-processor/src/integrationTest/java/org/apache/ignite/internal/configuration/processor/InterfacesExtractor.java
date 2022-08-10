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

package org.apache.ignite.internal.configuration.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

/**
 * {@link ClassVisitor} that allows to extract class interfaces (with generics).
 */
class InterfacesExtractor extends ClassVisitor {

    /** Map from interface names to lists of their generic types. */
    private final Map<String, List<String>> interfacesMap = new HashMap<>();

    public InterfacesExtractor() {
        super(Opcodes.ASM7);
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
        if (signature != null) {
            parseSignature(signature);
        } else {
            for (String iface : interfaces) {
                interfacesMap.put(iface, List.of());
            }
        }
    }

    private void parseSignature(String signature) {
        SignatureReader signatureReader = new SignatureReader(signature);

        signatureReader.accept(new SignatureVisitor(Opcodes.ASM7) {

            @Override
            public SignatureVisitor visitInterface() {
                return new SignatureVisitor(Opcodes.ASM7) {
                    final List<String> currentInterface = new ArrayList<>();

                    @Override
                    public void visitClassType(String name) {
                        interfacesMap.put(name, currentInterface);
                    }

                    @Override
                    public SignatureVisitor visitTypeArgument(char wildcard) {
                        return new SignatureVisitor(Opcodes.ASM7) {
                            @Override
                            public void visitClassType(String name) {
                                currentInterface.add(name);
                            }
                        };
                    }
                };
            }
        });
    }

    /**
     * Returns map from interface names to lists of their generic types.
     */
    public Map<String, List<String>> interfaces() {
        return Map.copyOf(interfacesMap);
    }
}
