package io.github.braayy.utils;

import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStructInfo;

import java.util.Map;

public class RedbitQueryBuilders {

    public static String buildUpsertQuery(RedbitStructInfo structInfo, Map<String, String> valueMap) {
        StringBuilder builder = new StringBuilder("INSERT INTO `").append(structInfo.getName()).append("`(");
        boolean first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            builder.append(!first ? ", " : "").append('`').append(columnInfo.getName()).append('`');

            first = false;
        }
        builder.append(") VALUES (");
        first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            String value = valueMap.get(columnInfo.getName());
            if (value == null)
                value = columnInfo.getDefaultValue();

            builder.append(!first ? ", " : "").append('\'').append(RedbitUtils.escapeToSql(value)).append('\'');

            first = false;
        }
        builder.append(") ON DUPLICATE KEY UPDATE ");
        first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getColumns()) {
            String value = valueMap.get(columnInfo.getName());
            if (value == null)
                value = columnInfo.getDefaultValue();

            builder.append(!first ? ", " : "").append('`').append(columnInfo.getName()).append("`='").append(RedbitUtils.escapeToSql(value)).append('\'');

            first = false;
        }
        return builder.toString();
    }

    public static String buildDeleteQuery(RedbitStructInfo structInfo, String idValue) {
        StringBuilder builder = new StringBuilder("DELETE FROM `").append(structInfo.getName()).append("` WHERE ");
        RedbitColumnInfo idColumn = structInfo.getIdColumn();

        builder.append('`').append(idColumn.getName()).append("`='").append(idValue).append('\'');

        return builder.toString();
    }

    public static String buildCreateTableQuery(RedbitStructInfo structInfo) {
        StringBuilder builder = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(structInfo.getName()).append("`(");

        boolean first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            builder.append(!first ? ", " : "").append(columnInfo.getName()).append(" ").append(columnInfo.getSqlCreation());

            first = false;
        }

        RedbitColumnInfo idColumn = structInfo.getIdColumn();
        builder.append(", PRIMARY KEY(").append(idColumn.getName()).append(')');

        builder.append(")");

        return builder.toString();
    }

}
