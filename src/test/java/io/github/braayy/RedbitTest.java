package io.github.braayy;

import java.util.concurrent.TimeUnit;

public class RedbitTest {

    public static void main(String[] args) {
        RedbitConfig config = new RedbitConfig();
        config.setRedisHost("localhost");
        config.setRedisPort(6379);
        config.setMysqlHost("localhost");
        config.setMysqlPort(3306);
        config.setMysqlUser("root");
        config.setMysqlPassword("root");
        config.setMysqlDatabase("rabbitholemc");
        config.setDebug(true);
        config.setSyncDelay(TimeUnit.SECONDS.toMillis(5));

        Redbit.getStructRegistry().registerTable("users", UsersStruct.class);
        Redbit.init(config);

        try (UsersStruct users = new UsersStruct()) {
            users.uuid = "batatinha";
            users.name = "Braayy";
            users.age = 19;

            users.upsert();
        }

        try {
            Redbit.stop();
        } catch (InterruptedException exception) {
            exception.printStackTrace();
        }
    }

}
