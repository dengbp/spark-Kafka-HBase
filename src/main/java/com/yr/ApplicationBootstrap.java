package com.yr;

import com.yr.flume.Channel;
import com.yr.utils.AnnotionUtils;

/**
 * @author dengbp
 * @ClassName applicationbootstrap
 * @Description TODO
 * @date 2020-03-10 14:02
 */
public class ApplicationBootstrap {

    public static void main(String[] args) {
        AnnotionUtils.triggerRunner();
        Channel.sourceBuild(ApplicationBootstrap.class).sink();
    }
}
