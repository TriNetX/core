package com.trinetx.core.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler
 *
 * Created by yongdengchen on 8/29/14.
 */
public class Scheduler {

    private static ListeningScheduledExecutorService s_Scheduler;
    private static ListeningExecutorService          s_Executor;

    static {
        s_Scheduler = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(100));
        s_Executor = s_Scheduler;
    }

    public static <V> ListenableFuture<V> submit(Callable<V> callable) {
        return s_Executor.submit(callable);
    }

    public static void execute(Runnable command) {
        s_Scheduler.execute(command);
    }

    public static ListenableScheduledFuture schedule(Callable command, long delay, TimeUnit unit) {
        return s_Scheduler.schedule(command, delay, unit);
    }

    public static ListenableScheduledFuture schedule(Runnable command, long delay, TimeUnit unit) {
        return s_Scheduler.schedule(command, delay, unit);
    }

    public static ListenableScheduledFuture scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return s_Scheduler.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public static ListenableScheduledFuture scheduleWithFixedDelay(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return s_Scheduler.scheduleWithFixedDelay(command, initialDelay, period, unit);
    }
}
