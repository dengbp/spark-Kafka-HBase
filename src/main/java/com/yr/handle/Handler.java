package com.yr.handle;

import com.yr.vo.Event;

import java.io.Serializable;

/**
 * @author dengbp
 * @ClassName Handler
 * @Description 定义处理接口
 * @date 2020-03-10 10:19
 */
public interface Handler extends Serializable {

    Class<? extends Handler> getType();
    /**
     * Description 处理接口
     * @param event event
     * @return void
     * @Author dengbp
     * @Date 10:22 2020-03-10
     **/
    void handle(Event event);
}
