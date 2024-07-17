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

package org.apache.ignite.internal.catalog;

/**
 * This exception is thrown when an index is not found in the schema.
 *
 * <p>Example: This exception is used to properly handle IF EXISTS flag in ddl command handler.</p>
 */
public class IndexNotFoundValidationException extends CatalogValidationException {
    private static final long serialVersionUID = -9202116446922771598L;

    public IndexNotFoundValidationException(String message) {
        super(message);
    }
}
