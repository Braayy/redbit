package io.github.braayy;

import com.zaxxer.hikari.HikariDataSource;
import io.github.braayy.struct.RedbitStructInfo;
import io.github.braayy.synchronization.RedbitSynchronizationTimer;
import io.github.braayy.synchronization.RedbitSynchronizer;
import io.github.braayy.utils.RedbitQueryBuilders;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Redbit {

    public static final String KEY_FORMAT = "%s:%s";
    private static final Redbit instance = new Redbit();

    public static Redbit getInstance() {
        return instance;
    }

    private Redbit() {}

    private final RedbitStructRegistry structRegistry = new RedbitStructRegistry();
    private final Logger logger = Logger.getLogger("Redbit Logger");
    private RedbitSynchronizationTimer synchronizationTimer;
    private RedbitSynchronizer synchronizer;
    private JedisPooled jedis;
    private HikariDataSource dataSource;
    private RedbitConfig config;

    public static void init(RedbitConfig config) {
        instance.config = config;

        instance.synchronizationTimer = new RedbitSynchronizationTimer();
        instance.synchronizer = new RedbitSynchronizer();

        instance.jedis = new JedisPooled(config.getRedisHost(), config.getRedisPort());

        instance.dataSource = new HikariDataSource();
        instance.dataSource.setJdbcUrl(String.format("jdbc:mysql://%s:%d/%s", config.getMysqlHost(), config.getMysqlPort(), config.getMysqlDatabase()));
        instance.dataSource.setUsername(config.getMysqlUser());
        instance.dataSource.setPassword(config.getMysqlPassword());
        instance.dataSource.addDataSourceProperty("CachePrepStmts", "true");
        instance.dataSource.addDataSourceProperty("PrepStmtCacheSize", "250");
        instance.dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        instance.createTablesForStructs();

        instance.synchronizationTimer.start();
    }

    public static void stop() throws InterruptedException {
        instance.synchronizationTimer.interrupt();
        instance.synchronizationTimer.join();
        instance.dataSource.close();
    }

    private void createTablesForStructs() {
        for (RedbitStructInfo structInfo : structRegistry.getStructs()) {
            String stringQuery = RedbitQueryBuilders.buildCreateTableQuery(structInfo);
            try (RedbitQuery query = sqlQuery(stringQuery)) {
                query.executeUpdate();
            } catch (Exception exception) {
                Redbit.getLogger().log(Level.SEVERE, "Something went wrong while creating table " + structInfo.getName(), exception);
            }
        }
    }

    public static RedbitStructRegistry getStructRegistry() {
        return instance.structRegistry;
    }

    public static RedbitSynchronizer getSynchronizer() {
        return instance.synchronizer;
    }

    public static Logger getLogger() {
        return instance.logger;
    }

    @Nullable
    public static JedisPooled getJedis() {
        return instance.jedis;
    }

    public static RedbitConfig getConfig() {
        return instance.config;
    }

    public static RedbitQuery sqlQuery(String query) throws SQLException {
        Objects.requireNonNull(instance.dataSource, "Hikari was not initialized yet! Redbit#init(RedbitConfig) should do it");

        Connection connection = instance.dataSource.getConnection();
        PreparedStatement stmt = connection.prepareStatement(query);

        return new RedbitQuery(connection, stmt);
    }
}
