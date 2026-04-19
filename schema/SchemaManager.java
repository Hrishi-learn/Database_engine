package schema;

import storage_engine.KeyValueStore;

import java.util.concurrent.ConcurrentHashMap;

public class SchemaManager {
    ConcurrentHashMap<String,String> schema = new ConcurrentHashMap<>();
    private static SchemaManager INSTANCE;

    /*
    * To prevent from creating new SchemaManager.
    * */
    private SchemaManager(){}

    public static void initialize() {
        if (INSTANCE == null) {
            INSTANCE = new SchemaManager();
        }
    }

    public static SchemaManager getInstance() {
        return INSTANCE;
    }
    public ConcurrentHashMap<String,String> getSchema(){
        return schema;
    }
}
