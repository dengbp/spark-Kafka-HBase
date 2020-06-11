package com.yr.handle;

import com.yr.common.Register;
import com.yr.handle.hbase.HBaseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dengbp
 * @ClassName DefaultHandlerManager
 * @Description TODO
 * @date 2020-03-10 15:13
 */
public class DefaultHandlerManager implements HandlerManager{
    private static Logger logger = LoggerFactory.getLogger(DefaultHandlerManager.class);

    private Map<Class<? extends Handler>, Handler> mapHandlers = new ConcurrentHashMap<>();
    private final Register register;
    public DefaultHandlerManager(Register register){
        /** 注册所有处理器的地方 */
        this.register = register;
        Handler hBaseHandler = new HBaseHandler(this.register);
        register(hBaseHandler);
    }

    @Override
    public void register(Handler handler) {
        logger.info("注册处理器[{}]",handler.getType());
        mapHandlers.put(handler.getType(), handler);
    }

    @Override
    public <T extends Handler> T handler(Class<T> clazz){
        Handler provider = mapHandlers.get(clazz);
        if (provider != null) {
            return (T) provider;
        } else {
            logger.error("No handler [{}] found in DefaultHandlerManager, please make sure it is registered in DefaultHandlerManager ",
                    clazz.getName());
            return (T) NullHandler.provider;
        }
    }

}
