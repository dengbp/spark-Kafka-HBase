package com.yr.router.strategy;

import java.io.Serializable;

/**
 * @author dengbp
 * @ClassName Strategy
 * @Description TODO
 * @date 2020-03-12 11:16
 */
public interface ConnectStrategy extends Serializable {

    /**
     * Description todo
     * @param
     * @return java.lang.Class<? extends com.structured.write.strategy.Strategy>
     * @Author dengbp
     * @Date 11:20 2020-03-12
     **/
    Class<? extends ConnectStrategy> getType();

    /**
     * Description todo
     * @param
     * @return void
     * @Author dengbp
     * @Date 11:30 2020-03-12
     **/
    void open();

    /**
     * Description todo
     * @param
     * @return java.lang.Object
     * @Author dengbp
     * @Date 14:47 2020-03-12
     **/
    Object connection();

   /**
    * Description todo
    * @param conn
    * @return void
    * @Author dengbp
    * @Date 17:56 2020-03-12
    **/
    void returnConnect(Object conn);
}
