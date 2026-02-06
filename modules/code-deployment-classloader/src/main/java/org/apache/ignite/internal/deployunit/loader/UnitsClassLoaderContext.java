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

package org.apache.ignite.internal.deployunit.loader;

import java.util.function.Consumer;

/**
 * Job context.
 */
public class UnitsClassLoaderContext implements AutoCloseable {

    private final UnitsClassLoader classLoader;

    private final Consumer<UnitsClassLoaderContext> onClose;

    public UnitsClassLoaderContext(UnitsClassLoader classLoader, Consumer<UnitsClassLoaderContext> onClose) {
        this.classLoader = classLoader;
        this.onClose = onClose;
    }

    public UnitsClassLoader classLoader() {
        return classLoader;
    }

    @Override
    public void close() {
        onClose.accept(this);
    }
}
