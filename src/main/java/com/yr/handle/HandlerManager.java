package com.yr.handle;

/**
 * @author dengbp
 * @ClassName HandlerManager
 * @Description TODO
 * @date 2020-03-10 14:59
 */

public interface HandlerManager {

    public void register(Handler handler);

    public <T extends Handler> T handler(Class<T> clazz);
}
