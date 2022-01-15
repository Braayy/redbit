package io.github.braayy.fetch;

import io.github.braayy.Redbit;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStruct;
import io.github.braayy.struct.RedbitStructInfo;
import io.github.braayy.utils.RedbitUtils;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.JedisPooled;

import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

public class RedbitSingleRedisFetch extends RedbitRedisFetch {

    private String key;

    public RedbitSingleRedisFetch(RedbitStruct struct, @NotNull String key) {
        super(struct, null);
        this.key = key;
    }

    @Override
    public Result next() {
        try {
            if (this.key == null) return Result.COMPLETE;

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            Map<String, String> valueMap = jedis.hgetAll(this.key);

            if (valueMap.isEmpty())
                return Result.NOT_FOUND;

            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(struct.getClass());
            Objects.requireNonNull(structInfo, "Struct " + struct.getClass().getSimpleName() + " was not registered!");

            String idValue = this.key.split(":", 2)[1];
            RedbitColumnInfo idColumn = structInfo.getIdColumn();
            valueMap.put(idColumn.getName(), idValue);

            RedbitUtils.setFieldsValueFromRedis(structInfo, struct, valueMap);
            this.key = null;

            return Result.FOUND;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return Result.ERROR;
        }
    }
}
