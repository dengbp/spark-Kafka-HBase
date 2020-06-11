package com.yr.annotation;

import java.lang.annotation.*;

/**
 * @author dengbp
 * @ClassName Runner
 * @Description TODO
 * @date 2020-03-14 14:22
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Runner {

    String name() default "";
}
