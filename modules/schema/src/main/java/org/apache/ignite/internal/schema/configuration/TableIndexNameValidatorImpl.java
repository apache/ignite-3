package org.apache.ignite.internal.schema.configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;

public class TableIndexNameValidatorImpl implements Validator<TableIndexNameValidator, NamedListView<TableIndexView>> {
    /** Static instance. */
    public static final TableIndexNameValidatorImpl INSTANCE = new TableIndexNameValidatorImpl();

    @Override
    public void validate(TableIndexNameValidator annotation, ValidationContext<NamedListView<TableIndexView>> ctx) {
        System.out.println(">XXX> VALIDATE " + ctx.currentKey() + ", " + ctx.getNewValue().namedListKeys());

        TablesView tablesConfig = ctx.getNewRoot(TablesConfiguration.KEY);

        if (tablesConfig != null) {
            Set<String> names = new HashSet<>(annotation.type() == 0
                    ? tablesConfig.tables().namedListKeys()
                    : tablesConfig.indexes().namedListKeys());

//            NamedListView<? extends TableIndexView> idxs = tablesConfig.indexes();


//            HashSet<String> tblNames = new HashSet<>(tbls.namedListKeys());

//            for (String idxName : idxs.namedListKeys()) {
//                if (tblNames.contains(idxName))
//                    ctx.addIssue(new ValidationIssue(key, "Found index with table name [name=" + idxName + ']');
//            }

            String str = annotation.type() == 0 ? "index. Table" : "table. Index";

            for (String key : newKeys(ctx.getOldValue(), ctx.getNewValue())) {
                if (names.contains(key)) {
//                    System.out.println("Cannot create index, table with the same name already exists [name=" + key + ']');

                    ctx.addIssue(new ValidationIssue(key, String.format("Cannot create %s with the same name already exists", str)));
                }
            }
        }
    }

    private List<String> newKeys(NamedListView<?> before, NamedListView<?> after) {
        List<String> result = new ArrayList<>(after.namedListKeys());

        if (before != null) {
            result.removeAll(before.namedListKeys());
        }

        return result;
    }
}
