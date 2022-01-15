package io.github.braayy.struct;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.fetch.RedbitDatabaseFetch;
import io.github.braayy.fetch.RedbitFetch;
import io.github.braayy.fetch.RedbitRedisFetch;
import io.github.braayy.fetch.RedbitSingleRedisFetch;
import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import io.github.braayy.utils.RedbitQueryBuilders;
import io.github.braayy.utils.RedbitRedisScanner;
import io.github.braayy.utils.RedbitUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;

import java.util.*;
import java.util.logging.Level;

public class RedbitVolatileStruct extends RedbitStruct {

    @Override
    public boolean insert() {
        return insert(true);
    }

    public boolean insert(boolean synchronize) {
        return upsert(false, synchronize);
    }

    public boolean update() {
        return update(true);
    }

    public boolean update(boolean synchronize) {
        return upsert(true, synchronize);
    }

    private boolean upsert(boolean ignoreNullValues, boolean synchronize) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (synchronize && Redbit.getSynchronizer().isShuttingDown())
                throw new IllegalStateException("Synchronized operation ran while synchronizer is shutting down");

            Map<String, String> valueMap = RedbitUtils.getStructValues(structInfo, this, ignoreNullValues);

            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.hset(key, valueMap);

            if (synchronize)
                Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.UPSERT);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public boolean deleteById() {
        return deleteById(true);
    }

    public boolean deleteById(boolean synchronize) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (synchronize && Redbit.getSynchronizer().isShuttingDown())
                throw new IllegalStateException("Synchronized operation ran while synchronizer is shutting down");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.del(key);

            if (synchronize)
                Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.DELETE);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public boolean deleteAll() {
        return deleteAll(true);
    }

    public boolean deleteAll(boolean synchronize) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (synchronize && Redbit.getSynchronizer().isShuttingDown())
                throw new IllegalStateException("Synchronized operation ran while synchronizer is shutting down");

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            ScanParams scanParams = new ScanParams().match(structInfo.getName() + ":*").count(10);
            RedbitRedisScanner scanner = new RedbitRedisScanner(scanParams);

            String nextKey;
            List<String> keys = new ArrayList<>();
            while ((nextKey = scanner.next()) != null) {
                keys.add(nextKey);
            }

            jedis.del(keys.toArray(new String[0]));

            if (synchronize)
                Redbit.getSynchronizer().addModifiedKey(structInfo, null, Operation.DELETE_ALL);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    @NotNull
    public RedbitFetch.Result fetchById() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            Objects.requireNonNull(structInfo, "Struct " + getClass().getSimpleName() + " was not registered!");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            try (RedbitSingleRedisFetch fetch = new RedbitSingleRedisFetch(this, key)) {
                RedbitFetch.Result result = fetch.next();

                switch (result) {
                    case FOUND:
                    case ERROR:
                        return result;
                }
            }

            String strQuery = RedbitQueryBuilders.buildSelectByIdQuery(structInfo, idValue);
            RedbitQuery query = Redbit.sqlQuery(strQuery);
            try (RedbitDatabaseFetch fetch = new RedbitDatabaseFetch(this, query)) {
                RedbitFetch.Result result = fetch.next();

                if (result == RedbitFetch.Result.FOUND) {
                    upsert(false, false);
                }

                return result;
            }
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return RedbitFetch.Result.ERROR;
        }
    }

    @Override
    @Nullable
    public RedbitRedisFetch fetchAll() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            Objects.requireNonNull(structInfo, "Struct " + getClass().getSimpleName() + " was not registered!");

            ScanParams scanParams = new ScanParams().match(structInfo.getName() + ":*").count(10);
            return new RedbitRedisFetch(this, scanParams);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return null;
        }
    }

    @Nullable
    public RedbitDatabaseFetch fetchAllFromDatabase() {
        return ((RedbitDatabaseFetch) fetchWhere(null));
    }

}
