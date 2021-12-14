package io.github.braayy;

public class RedbitConfig {

    private String redisHost, mysqlHost, mysqlDatabase, mysqlUser, mysqlPassword;
    private int redisPort, mysqlPort;
    private boolean debug;
    private long syncDelay;
    private int parallelTasks;

    public RedbitConfig() {
    }

    public RedbitConfig(String redisHost, String mysqlHost, String mysqlDatabase, String mysqlUser, String mysqlPassword, int redisPort, int mysqlPort, boolean debug, long syncDelay, int parallelTasks) {
        this.redisHost = redisHost;
        this.mysqlHost = mysqlHost;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
        this.redisPort = redisPort;
        this.mysqlPort = mysqlPort;
        this.debug = debug;
        this.syncDelay = syncDelay;
        this.parallelTasks = parallelTasks;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    public void setMysqlHost(String mysqlHost) {
        this.mysqlHost = mysqlHost;
    }

    public String getMysqlDatabase() {
        return mysqlDatabase;
    }

    public void setMysqlDatabase(String mysqlDatabase) {
        this.mysqlDatabase = mysqlDatabase;
    }

    public String getMysqlUser() {
        return mysqlUser;
    }

    public void setMysqlUser(String mysqlUser) {
        this.mysqlUser = mysqlUser;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    public void setMysqlPort(int mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public long getSyncDelay() {
        return syncDelay;
    }

    public void setSyncDelay(long syncDelay) {
        this.syncDelay = syncDelay;
    }

    public int getParallelTasks() {
        return parallelTasks;
    }

    public void setParallelTasks(int parallelTasks) {
        this.parallelTasks = parallelTasks;
    }
}
