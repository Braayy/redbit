package io.github.braayy.struct;

import io.github.braayy.Redbit;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

public class RedbitStruct implements AutoCloseable {

    public boolean upsert() {
        return upsert(false);
    }

    public boolean upsert(boolean ignoreNullValues) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null) {
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");
            }

            Map<String, String> valueMap = getStructValues(structInfo, ignoreNullValues);

            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (idValue == null || Objects.equals(idValue, "")) {
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());
            }

            Jedis jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.hset(key, valueMap);

            Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.UPSERT);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean delete() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null) {
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");
            }

            String idValue = getIdValue(structInfo);
            if (idValue == null || Objects.equals(idValue, "")) {
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());
            }

            Jedis jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format("%s:%s", structInfo.getName(), idValue);
            jedis.del(key);

            Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.DELETE);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean get() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null) {
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");
            }

            String idValue = getIdValue(structInfo);
            if (idValue == null || Objects.equals(idValue, "")) {
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());
            }

            Jedis jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format("%s:%s", structInfo.getName(), idValue);
            Map<String, String> valueMap = jedis.hgetAll(key);

            for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                RedbitColumnInfo columnInfo = structInfo.getColumnFromName(entry.getKey());
                if (columnInfo == null)
                    throw new IllegalArgumentException("Could not find column " + entry.getKey() + " in struct " + structInfo.getName());

                Field field = getClass().getDeclaredField(columnInfo.getFieldName());
                field.setAccessible(true);

                setFieldValue(field, entry.getValue());
            }

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public void close() {

    }

    private void setFieldValue(Field field, String value) throws IllegalAccessException, IllegalArgumentException {
        Class<?> type = field.getType();

        if (type.equals(Byte.class))
            field.set(this, Byte.parseByte(value));
        else if (type.equals(Character.class))
            field.set(this, value.charAt(0));
        else if (type.equals(Short.class))
            field.set(this, Short.parseShort(value));
        else if (type.equals(Integer.class))
            field.set(this, Integer.parseInt(value));
        else if (type.equals(Long.class))
            field.set(this, Long.parseLong(value));
        else if (type.equals(String.class))
            field.set(this, value);
        else
            throw new IllegalArgumentException(type.getName() + " type is not supported by redbit");
    }

    private Map<String, String> getStructValues(RedbitStructInfo structInfo, boolean ignoreNullValues) throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> valueMap = new HashMap<>();
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            Field field = getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);
            Object value = field.get(this);

            if (value == null && ignoreNullValues) continue;

            if (value == null)
                value = columnInfo.getDefaultValue();

            valueMap.put(columnInfo.getName(), value.toString());
        }

        return valueMap;
    }

    private String getIdValue(RedbitStructInfo structInfo) throws NoSuchFieldException, IllegalAccessException {
        RedbitColumnInfo columnInfo = structInfo.getIdColumn();
        Field field = getClass().getDeclaredField(columnInfo.getFieldName());
        field.setAccessible(true);
        Object value = field.get(this);

        if (value == null)
            throw new IllegalArgumentException("Null value for id in table " + structInfo.getName());

        return value.toString();
    }

}
