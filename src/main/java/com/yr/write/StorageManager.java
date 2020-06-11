package com.yr.write;

import com.yr.po.hbase.SchemaData;
import com.yr.write.hbase.HBaseStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dengbp
 * @ClassName StorageManager
 * @Description TODO
 * @date 2020-03-10 10:28
 */
public final class StorageManager implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(StorageManager.class);
    private List<Storage> storages = new ArrayList<>();
    private static Map<Class<? extends Storage>,Storage> storageCache = new IdentityHashMap();
    public static StorageManager instance = new StorageManager();

    private StorageManager(){
          Storage hBaseStorage = new HBaseStorage(new SchemaData());
          this.register(hBaseStorage);
    }

    public void register(Storage storage){
        storageCache.put(storage.getType(),storage);
    }

    public Storage create(Class<? extends Storage> c,boolean isSingle){
        Storage storage = null;
        try {
            if (isSingle){
                storage = storageCache.containsKey(c.getClass())?storageCache.get(c.getClass()):c.newInstance();
            }else {
                storage = c.newInstance();
            }
            storageCache.put(storage.getClass(),storage);
            storages.add(storage);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }finally {
            return storage;
        }

    }

    public <T extends Storage> T storage(Class<T> clazz){
        Storage provider = storageCache.get(clazz);
        if (provider != null) {
            return (T) provider;
        } else {
            logger.error("No storage [{}] found in StorageManager, please make sure it is registered in StorageManager ",
                    clazz.getName());
            return (T) NullStorage.storage;
        }
    }

    public int storageSize(){
        return storages.size();
    }

}
