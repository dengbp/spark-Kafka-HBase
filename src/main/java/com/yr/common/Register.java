package com.yr.common;

/**
 * @author dengbp
 * @ClassName Register
 * @Description 公共注册器抽象
 * @date 2020-03-10 11:16
 */
public interface Register<T> {

    /**
     * Description todo
     * @param t t
     * @return void
     * @Author dengbp
     * @Date 11:20 2020-03-10
     **/
    public void signUp(T t);

    /**
     * Description todo
     * @param
     * @return void
     * @Author dengbp
     * @Date 11:21 2020-03-10
     **/
    public void notifyAllHandler();
}
