package com.yr.handle;

import com.yr.common.Register;
import com.yr.vo.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengbp
 * @ClassName NullHandler
 * @Description TODO
 * @date 2020-03-10 15:04
 */
public class NullHandler  extends AbstractHandler{
    private static Logger logger = LoggerFactory.getLogger(NullHandler.class);
    public static final NullHandler provider = new NullHandler(null);

    public NullHandler(Register register) {
        super(register);
    }

    @Override
    public Class<? extends Handler> getType() {
        return null;
    }

    @Override
    public void handle(Event event) {
        logger.warn("do nothing...");
    }
}
