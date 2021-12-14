package io.github.braayy.synchronization;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.utils.RedbitQueryBuilders;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

public class RedbitSynchronizationTask implements Runnable {

    private final RedbitSynchronizationEntry entry;

    public RedbitSynchronizationTask(RedbitSynchronizationEntry entry) {
        this.entry = entry;
    }

    @Override
    public void run() {
        try {
            String stringQuery;
            if (entry.getOperation() == RedbitSynchronizationEntry.Operation.UPSERT) {
                Jedis jedis = Redbit.getJedis();
                Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

                String key = String.format(Redbit.KEY_FORMAT, entry.getStructInfo().getName(), entry.getIdValue());
                Map<String, String> valueMap = jedis.hgetAll(key);
                valueMap.put(entry.getStructInfo().getIdColumn().getName(), entry.getIdValue());

                stringQuery = RedbitQueryBuilders.buildUpsertQuery(entry.getStructInfo(), valueMap);
            } else
                stringQuery = RedbitQueryBuilders.buildDeleteQuery(entry.getStructInfo(), entry.getIdValue());

            try (RedbitQuery query = Redbit.sqlQuery(stringQuery)) {
                query.executeUpdate();
            }
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while synchronizing key " + entry.getIdValue(), exception);
        }
    }

}
