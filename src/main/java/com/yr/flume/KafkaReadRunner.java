package com.yr.flume;

import com.yr.annotation.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengbp
 * @ClassName KafkaReadRunner
 * @Description TODO
 * @date 2020-03-13 11:58
 */
@Runner(name = "fafkaReadRunner")
public class KafkaReadRunner implements ModuleRunner{
    private static Logger logger = LoggerFactory.getLogger(KafkaReadRunner.class);

    @Override
    public void run(Object ...object) throws Exception {
        logger.info("KafkaReadRunner module start...");
    }
}
