package io.github.braayy.synchronization;

import io.github.braayy.Redbit;

public class RedbitSynchronizationTimer extends Thread {

    private long nextRun;

    @Override
    public synchronized void start() {
        super.start();

        nextRun = System.currentTimeMillis() + Redbit.getConfig().getSyncDelay();
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            if (nextRun - System.currentTimeMillis() >= 0) continue;
            nextRun = System.currentTimeMillis() + Redbit.getConfig().getSyncDelay();

            Redbit.getSynchronizer().synchronize();
        }
    }
}
