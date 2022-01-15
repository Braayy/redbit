package io.github.braayy.fetch;

import io.github.braayy.Redbit;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStruct;
import io.github.braayy.struct.RedbitStructInfo;
import io.github.braayy.utils.RedbitRedisScanner;
import io.github.braayy.utils.RedbitUtils;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;

import java.util.*;
import java.util.logging.Level;

public class RedbitRedisFetch extends RedbitFetch {

    private final RedbitRedisScanner scanner;

    public RedbitRedisFetch(RedbitStruct struct, ScanParams scanParams) {
        super(struct);
        this.scanner = new RedbitRedisScanner(scanParams);
    }

    @Override
    public Result next() {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String nextKey = this.scanner.next();

            if (nextKey == null)
                return Result.COMPLETE;

            Map<String, String> valueMap = jedis.hgetAll(nextKey);

            if (valueMap.isEmpty())
                return Result.NOT_FOUND;

            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(struct.getClass());
            Objects.requireNonNull(structInfo, "Struct " + struct.getClass().getSimpleName() + " was not registered!");

            String idValue = nextKey.split(":", 2)[1];
            RedbitColumnInfo idColumn = structInfo.getIdColumn();
            valueMap.put(idColumn.getName(), idValue);

            RedbitUtils.setFieldsValueFromRedis(structInfo, struct, valueMap);

            return Result.FOUND;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return Result.ERROR;
        }
    }

    @Override
    public void close() {}
}
