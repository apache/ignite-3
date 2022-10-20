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

package org.apache.ignite.internal.schema.configuration;

import java.util.Objects;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Column definition validator implementation validates column changes.
 */
public class ColumnTypeValidatorImpl implements Validator<ColumnTypeValidator, ColumnTypeView> {
    /** Static instance. */
    public static final ColumnTypeValidatorImpl INSTANCE = new ColumnTypeValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(ColumnTypeValidator annotation, ValidationContext<ColumnTypeView> ctx) {
        ColumnTypeView newType = ctx.getNewValue();
        ColumnTypeView oldType = ctx.getOldValue();

        validateLength(ctx, newType);
        validatePrecision(ctx, newType);
        validateScale(ctx, newType);

        if (oldType == null) {
            return; // Nothing to do.
        }

        if (!Objects.deepEquals(newType.type(), oldType.type())
                || newType.precision() != oldType.precision()
                || newType.scale() != oldType.scale()
                || newType.length() != oldType.length()) {
            ColumnView columnView = ctx.getNewOwner();

            assert columnView != null;

            ctx.addIssue(new ValidationIssue(columnView.name(), "Column type can't be changed"));
        }

    }

    private void validateLength(ValidationContext<ColumnTypeView> ctx, ColumnTypeView typeView) {
        switch (typeView.type()) {
            case "STRING":
            case "BYTES":
            case "BITMASK":
                if (typeView.length() <= 0) {
                    ColumnView columnView = ctx.getNewOwner();

                    assert columnView != null;

                    ctx.addIssue(new ValidationIssue(columnView.name(), "Length must be positive"));
                }

                break;
            default:
                // nothing to do
                break;
        }
    }

    private void validatePrecision(ValidationContext<ColumnTypeView> ctx, ColumnTypeView typeView) {
        switch (typeView.type()) {
            case "NUMBER":
            case "DECIMAL":
                if (typeView.precision() <= 0) {
                    ColumnView columnView = ctx.getNewOwner();

                    assert columnView != null;

                    ctx.addIssue(new ValidationIssue(columnView.name(), "Precision must be positive"));
                }

                break;

            case "TIME":
            case "DATETIME":
            case "TIMESTAMP":
                if (typeView.precision() < 0 || typeView.precision() > 9) {
                    ColumnView columnView = ctx.getNewOwner();

                    assert columnView != null;

                    ctx.addIssue(new ValidationIssue(columnView.name(), "Precision must be in range [0-9]"));
                }

                break;
            default:
                // nothing to do
                break;
        }
    }

    private void validateScale(ValidationContext<ColumnTypeView> ctx, ColumnTypeView typeView) {
        if (!"DECIMAL".equals(typeView.type())) {
            return;
        }

        if (typeView.precision() < typeView.scale()) {
            ColumnView columnView = ctx.getNewOwner();

            assert columnView != null;

            ctx.addIssue(new ValidationIssue(columnView.name(), "Scale must be lower or equal to precision"));
        }
    }

    /** Private constructor. */
    private ColumnTypeValidatorImpl() {
    }
}
