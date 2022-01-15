package io.github.braayy.utils;

import io.github.braayy.Redbit;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStructInfo;

import java.util.Map;

public class RedbitQueryBuilders {

    public static String buildUpsertQuery(RedbitStructInfo structInfo, Map<String, String> valueMap, boolean ignoreNullColumns) {
        StringBuilder builder = new StringBuilder("INSERT INTO `").append(structInfo.getName()).append("`(");
        boolean first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            String value = valueMap.get(columnInfo.getName());
            if (RedbitUtils.isNullString(value) && ignoreNullColumns) continue;

            builder.append(!first ? ", " : "").append('`').append(columnInfo.getName()).append('`');

            first = false;
        }
        builder.append(") VALUES (");

        first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            String value = valueMap.get(columnInfo.getName());
            if (RedbitUtils.isNullString(value) && ignoreNullColumns) continue;

            builder.append(!first ? ", " : "");

            if (RedbitUtils.isNullString(value))
                builder.append("DEFAULT");
            else
                builder.append('\'').append(RedbitUtils.escapeToSql(value)).append('\'');

            first = false;
        }
        builder.append(") ON DUPLICATE KEY UPDATE ");

        first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getColumns()) {
            String value = valueMap.get(columnInfo.getName());
            if (RedbitUtils.isNullString(value) && ignoreNullColumns) continue;

            builder.append(!first ? ", " : "").append('`').append(columnInfo.getName()).append("`=");

            if (RedbitUtils.isNullString(value))
                builder.append("DEFAULT");
            else
                builder.append('\'').append(RedbitUtils.escapeToSql(value)).append('\'');

            first = false;
        }

        String query = builder.toString();

        return query;
    }

    public static String buildDeleteQuery(RedbitStructInfo structInfo, String whereClause) {
        StringBuilder builder = new StringBuilder("DELETE FROM `").append(structInfo.getName()).append('`');

        if (whereClause != null)
            builder.append(" WHERE ").append(whereClause);

        String query = builder.toString();

        return query;
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

        String query = builder.toString();

        return query;
    }

    public static String buildSelectByIdQuery(RedbitStructInfo structInfo, String idValue) {
        RedbitColumnInfo idColumn = structInfo.getIdColumn();

        String query = "SELECT * FROM `" + structInfo.getName() + "` WHERE `" + idColumn.getName() + "`='" + RedbitUtils.escapeToSql(idValue) + "' LIMIT 1";

        return query;
    }

    public static String buildSelectByCustomWhere(RedbitStructInfo structInfo, String whereClause) {
        String query;

        if (whereClause == null)
            query = "SELECT * FROM `" + structInfo.getName() + '`';
        else
            query = "SELECT * FROM `" + structInfo.getName() + "` WHERE " + whereClause;

        return query;
    }

}
