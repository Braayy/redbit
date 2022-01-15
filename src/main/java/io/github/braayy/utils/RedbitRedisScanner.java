package io.github.braayy.utils;

import io.github.braayy.Redbit;
import io.github.braayy.fetch.RedbitFetch;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

public class RedbitRedisScanner {

    private final Queue<String> remainingKeys = new ArrayDeque<>();
    private final ScanParams scanParams;
    private String nextCursor;

    public RedbitRedisScanner(ScanParams scanParams) {
        this.scanParams = scanParams;
    }

    public String next() {
        String nextKey = this.remainingKeys.poll();

        if (nextKey == null) {
            if (this.nextCursor != null && this.nextCursor.equals("0")) return null;

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            ScanResult<String> scan = jedis.scan(Objects.toString(this.nextCursor, "0"), this.scanParams);
            this.remainingKeys.addAll(scan.getResult());
            this.nextCursor = scan.getCursor();

            nextKey = this.remainingKeys.poll();
        }

        return nextKey;
    }

}
