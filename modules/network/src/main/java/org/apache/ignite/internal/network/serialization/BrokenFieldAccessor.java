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

package org.apache.ignite.internal.network.serialization;

/**
 * A broken {@link FieldAccessor} that throws an exception on any method invocation to help diagnose programming errors.
 */
class BrokenFieldAccessor implements FieldAccessor {
    private final String fieldName;
    private final String declaringClassName;

    BrokenFieldAccessor(String fieldName, String declaringClassName) {
        this.fieldName = fieldName;
        this.declaringClassName = declaringClassName;
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(Object target, Object fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public byte getByte(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setByte(Object target, byte fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public short getShort(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setShort(Object target, short fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public int getInt(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setInt(Object target, int fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public long getLong(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setLong(Object target, long fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setFloat(Object target, float fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setDouble(Object target, double fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public char getChar(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setChar(Object target, char fieldValue) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public boolean getBoolean(Object target) {
        throw brokenException();
    }

    /** {@inheritDoc} */
    @Override
    public void setBoolean(Object target, boolean fieldValue) {
        throw brokenException();
    }

    private IllegalStateException brokenException() {
        return new IllegalStateException("Field " + declaringClassName + "#" + fieldName
                + " is missing locally, no accesses to it should be performed, this is a bug");
    }
}
