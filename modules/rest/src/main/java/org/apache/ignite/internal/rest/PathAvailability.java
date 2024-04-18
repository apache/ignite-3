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

package org.apache.ignite.internal.rest;

import org.jetbrains.annotations.Nullable;

/**
 * REST method path availability representation.
 */
public class PathAvailability {
    private final boolean isAvailable;

    @Nullable
    private final String unavailableTitle;

    @Nullable
    private final String unavailableReason;

    private PathAvailability(boolean isAvailable, @Nullable String unavailableTitle, @Nullable String unavailableReason) {
        this.isAvailable = isAvailable;
        this.unavailableTitle = unavailableTitle;
        this.unavailableReason = unavailableReason;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    @Nullable
    public String unavailableTitle() {
        return unavailableTitle;
    }

    @Nullable
    public String unavailableReason() {
        return unavailableReason;
    }

    public static PathAvailability available() {
        return new PathAvailability(true, null, null);
    }

    public static PathAvailability unavailable(String title, String reason) {
        return new PathAvailability(false, title, reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PathAvailability that = (PathAvailability) o;

        if (isAvailable != that.isAvailable) {
            return false;
        }
        if (unavailableTitle != null ? !unavailableTitle.equals(that.unavailableTitle) : that.unavailableTitle != null) {
            return false;
        }
        return unavailableReason != null ? unavailableReason.equals(that.unavailableReason) : that.unavailableReason == null;
    }

    @Override
    public int hashCode() {
        int result = (isAvailable ? 1 : 0);
        result = 31 * result + (unavailableTitle != null ? unavailableTitle.hashCode() : 0);
        result = 31 * result + (unavailableReason != null ? unavailableReason.hashCode() : 0);
        return result;
    }
}
