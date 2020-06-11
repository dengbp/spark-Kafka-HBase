package com.yr.write;

import java.io.Serializable;

/**
 * @author dengbp
 * @ClassName Storage
 * @Description 定义存储接口
 * @date 2020-03-10 10:41
 */
public interface Storage extends Serializable {

    /**
     * Description 存储接口抽象
     * @param
     * @return boolean
     * @Author dengbp
     * @Date 10:30 2020-03-10
     **/
    boolean storage();

    Class<? extends Storage> getType();
}
