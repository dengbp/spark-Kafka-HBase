package com.yr.write;

/**
 * @author dengbp
 * @ClassName NullStorge
 * @Description TODO
 * @date 2020-03-10 17:40
 */
public class NullStorage implements Storage{
    public static final  NullStorage storage = new NullStorage();

    @Override
    public boolean storage() {
        return false;
    }

    @Override
    public Class<? extends Storage> getType() {
        return null;
    }
}
