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

package org.apache.ignite.configuration.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation can only be applied to a class that is either marked with {@link ConfigurationRoot}
 * or the superclass is marked with {@link ConfigurationRoot}, {@link Config}.
 * <br><br/>
 * It indicates that this is an internal configuration that should be hidden from the end user.
 * Any extensions are allowed for any configuration.
 * <br><br/>
 * NOTE:
 * <ul>
 * <li>Extension and superclass fields cannot have the same name.</li>
 * <li>Extensions of the same superclass can have the same fields, they must be semantically equal,
 * then they will share it among themselves.</li>
 * </ul>
 *
 * @see ConfigurationRoot
 * @see Config
 */
@Target(TYPE)
@Retention(RUNTIME)
@Documented
public @interface InternalConfiguration {
}
