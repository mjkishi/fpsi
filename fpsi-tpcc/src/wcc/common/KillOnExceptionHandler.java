package wcc.common;

import java.lang.Thread.UncaughtExceptionHandler;


public class KillOnExceptionHandler implements UncaughtExceptionHandler {
    public void uncaughtException(Thread t, Throwable e) {
        e.printStackTrace();
        System.exit(1);
    }
}