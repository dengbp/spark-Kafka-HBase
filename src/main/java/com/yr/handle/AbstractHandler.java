package com.yr.handle;

import com.yr.common.Register;
import com.yr.write.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengbp
 * @ClassName AbstactHandler
 * @Description TODO
 * @date 2020-03-10 11:04
 */
public abstract class AbstractHandler  implements Handler{
    private static Logger logger = LoggerFactory.getLogger(AbstractHandler.class);

    public final StorageManager storageManager;

    public AbstractHandler(Register register) {
        storageManager = StorageManager.instance;
        logger.info("初始化存储管理器[{}]",storageManager.getClass());
        logger.info("注册观察者[{}]",this.getClass());
        register.signUp(this);
    }
}
