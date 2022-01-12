package io.github.braayy.synchronization;

import io.github.braayy.Redbit;

public class RedbitSynchronizationTimer extends Thread {

    private long nextRun = System.currentTimeMillis() + Redbit.getConfig().getSyncDelay();

    public RedbitSynchronizationTimer() {
        super("Redbit Synchronization Timer Thread");
        setDaemon(true);
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            if (nextRun - System.currentTimeMillis() >= 0) continue;
            Redbit.getSynchronizer().synchronize();

            nextRun = System.currentTimeMillis() + Redbit.getConfig().getSyncDelay();
        }
    }
}
