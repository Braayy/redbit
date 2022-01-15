package io.github.braayy.struct;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.fetch.RedbitDatabaseFetch;
import io.github.braayy.fetch.RedbitFetch;
import io.github.braayy.utils.RedbitQueryBuilders;
import io.github.braayy.utils.RedbitUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.logging.Level;

public class RedbitStruct {

    public boolean insert() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            Map<String, String> valueMap = RedbitUtils.getStructValues(structInfo, this, false);
            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            String strQuery = RedbitQueryBuilders.buildUpsertQuery(structInfo, valueMap, false);
            try (RedbitQuery query = Redbit.sqlQuery(strQuery)) {
                query.executeUpdate();
            }

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean deleteById() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            RedbitColumnInfo idColumn = structInfo.getIdColumn();

            String whereClause = "`" + idColumn.getName() + "`='" + RedbitUtils.escapeToSql(idValue) + '\'';

            return deleteWhere(whereClause);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean deleteAll() {
        return deleteWhere(null);
    }

    public boolean deleteWhere(String whereClause) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = RedbitQueryBuilders.buildDeleteQuery(structInfo, whereClause);
            try (RedbitQuery query = Redbit.sqlQuery(strQuery)) {
                query.executeUpdate();
            }

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @NotNull
    public RedbitFetch.Result fetchById() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());
            RedbitColumnInfo idColumn = structInfo.getIdColumn();

            String whereClause = "`" + idColumn.getName() + "`='" + RedbitUtils.escapeToSql(idValue) + "' LIMIT 1";

            try (RedbitFetch fetch = fetchWhere(whereClause)) {
                return fetch != null ? fetch.next() : RedbitFetch.Result.ERROR;
            }
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return RedbitFetch.Result.ERROR;
        }
    }

    @Nullable
    public RedbitFetch fetchAll() {
        return fetchWhere(null);
    }

    @Nullable
    public RedbitFetch fetchWhere(String whereClause) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = RedbitQueryBuilders.buildSelectByCustomWhere(structInfo, whereClause);

            RedbitQuery query = Redbit.sqlQuery(strQuery);
            query.executeQuery();

            return new RedbitDatabaseFetch(this, query);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return null;
        }
    }

    @Nullable
    public RedbitFetch fetchCustom(String customQuery) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = prepareCustomQuery(structInfo, customQuery);

            RedbitQuery query = Redbit.sqlQuery(strQuery);
            query.executeQuery();

            return new RedbitDatabaseFetch(this, query);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return null;
        }
    }

    private String prepareCustomQuery(RedbitStructInfo structInfo, String customQuery) {
        return customQuery.replace("{table}", structInfo.getName());
    }

    protected String getIdFieldValue(RedbitStructInfo structInfo) throws NoSuchFieldException, IllegalAccessException {
        RedbitColumnInfo columnInfo = structInfo.getIdColumn();
        Field field = getClass().getDeclaredField(columnInfo.getFieldName());
        field.setAccessible(true);

        Object value = field.get(this);
        return value != null ? value.toString() : null;
    }
}
