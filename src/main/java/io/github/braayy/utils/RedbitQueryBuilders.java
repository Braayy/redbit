package io.github.braayy.utils;

import io.github.braayy.Redbit;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStructInfo;

import java.util.Map;
import java.util.Objects;

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

            builder.append(!first ? ", " : "");

            if (Objects.equals(value, ""))
                builder.append("DEFAULT");
            else
                builder.append('\'').append(RedbitUtils.escapeToSql(value)).append('\'');

            first = false;
        }
        builder.append(") ON DUPLICATE KEY UPDATE ");

        first = true;
        for (RedbitColumnInfo columnInfo : structInfo.getColumns()) {
            String value = valueMap.get(columnInfo.getName());

            builder.append(!first ? ", " : "").append('`').append(columnInfo.getName()).append("`=");

            if (Objects.equals(value, ""))
                builder.append("DEFAULT");
            else
                builder.append('\'').append(RedbitUtils.escapeToSql(value)).append('\'');

            first = false;
        }

        String query = builder.toString();

        if (Redbit.getConfig().isDebug())
            Redbit.getLogger().info("[SQL] " + query);

        return query;
    }

    public static String buildDeleteQuery(RedbitStructInfo structInfo, String idValue) {
        StringBuilder builder = new StringBuilder("DELETE FROM `").append(structInfo.getName()).append("` WHERE ");
        RedbitColumnInfo idColumn = structInfo.getIdColumn();

        builder.append('`').append(idColumn.getName()).append("`='").append(RedbitUtils.escapeToSql(idValue)).append('\'');

        String query = builder.toString();

        if (Redbit.getConfig().isDebug())
            Redbit.getLogger().info("[SQL] " + query);

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

        if (Redbit.getConfig().isDebug())
            Redbit.getLogger().info("[SQL] " + query);

        return query;
    }

    public static String buildSelectQuery(RedbitStructInfo structInfo, String idValue) {
        RedbitColumnInfo idColumn = structInfo.getIdColumn();

        String query = "SELECT * FROM `" + structInfo.getName() + "` WHERE `" + idColumn.getName() + "`='" + RedbitUtils.escapeToSql(idValue) + "' LIMIT 1";

        if (Redbit.getConfig().isDebug())
            Redbit.getLogger().info("[SQL] " + query);

        return query;
    }

}
