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

package org.apache.ignite.internal.pagememory.io;

import java.util.Collection;

/**
 * Extension point for modules to provide their own {@link PageIO} implementations by implementing current interface and exporting
 * the implementation via corresponding {@code META-INF.services} resource.
 */
public interface PageIOModule {
    /**
     * @return A collection of {@link IOVersions} instances for all new page IO types in the module.
     */
    Collection<IOVersions<?>> ioVersions();
}
