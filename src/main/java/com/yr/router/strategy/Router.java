package com.yr.router.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dengbp
 * @ClassName Router
 * @Description TODO
 * @date 2020-03-12 11:16
 */
public final class Router implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(Router.class);

    private static Map<Class<? extends ConnectStrategy>, ConnectStrategy> strategyMap = new ConcurrentHashMap<>();
    public static Router instance = new Router();
    private final ConnectStrategy strategy;

    private Router() {
        this.strategy = new HbaseConnection();
    }

    public static void register(ConnectStrategy strategy) {
        logger.info("注册策略器[{}]",strategy.getType());
        strategyMap.put(strategy.getType(), strategy);
    }

    public ConnectStrategy defaultRouting(){
        return strategy;
    }

    public boolean init(){
        strategyMap.forEach((key,strategy)->{
            strategy.open();
        });
        return true;
    }

    public static ConnectStrategy routing(Class<? extends ConnectStrategy> type){
        return strategyMap.get(type);
    }
}
