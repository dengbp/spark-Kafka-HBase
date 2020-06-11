package com.yr.flume;

import com.yr.annotation.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengbp
 * @ClassName ForeachWriterRunner
 * @Description TODO
 * @date 2020-03-13 11:59
 */
@Runner(name = "foreachWriterRunner")
public class ForeachWriterRunner implements ModuleRunner{
    private static Logger logger = LoggerFactory.getLogger(ForeachWriterRunner.class);

    @Override
    public void run(Object... args) throws Exception {
        logger.info("ForeachWriter module start...");
    }
}
