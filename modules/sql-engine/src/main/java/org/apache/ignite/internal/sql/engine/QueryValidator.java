package org.apache.ignite.internal.sql.engine;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.exec.StatementMismatchException;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan.Type;

/**
 * The validator interface for checking actual query type against the expected query type.
 * Allows to validate both an existing plan and a newly parsed query.
 * */
public interface QueryValidator {
    /**
     * Checks if the expected query type is QUERY (SELECT).
     *
     * @return {@code true} if expected type is QUERY, {@code false} otherwise.
     */
    boolean isQuery();

    /**
     * Checks if 'explain' type is allowed.
     *
     * @return {@code true} if explain is allowed, {@code false} otherwise.
     */
    boolean allowExplain();

    /**
     * Checks if validation is needed.
     *
     * @return {@code true} if validation is not required, {@code false} otherwise.
     */
    boolean skipCheck();

    /**
     * Checks the prepared query plan type against the expected query type.
     *
     * @throws StatementMismatchException in the case of a validation error.
     */
    default void validatePlan(QueryPlan plan) throws StatementMismatchException {
        if (skipCheck()) {
            return;
        }
        if (plan.type() == Type.EXPLAIN) {
            if (allowExplain()) {
                return;
            }
            throw new StatementMismatchException();
        }
        if (plan.type() == Type.QUERY) {
            if (isQuery()) {
                return;
            }
            throw new StatementMismatchException();
        }
        if (!isQuery()) {
            return;
        }
        throw new StatementMismatchException();
    }

    /**
     * Checks the parsed query type against the expected query type.
     *
     * @throws StatementMismatchException in the case of a validation error.
     */
    default void validateParsedQuery(SqlNode rootNode) throws StatementMismatchException {
        if (skipCheck()) {
            return;
        }
        if (rootNode.getKind() == SqlKind.EXPLAIN) {
            if (allowExplain()) {
                return;
            }
            throw new StatementMismatchException();
        }
        if (rootNode.getKind() == SqlKind.SELECT) {
            if (isQuery()) {
                return;
            }
            throw new StatementMismatchException();
        }
        if (!isQuery()) {
            return;
        }
        throw new StatementMismatchException();
    }
}
