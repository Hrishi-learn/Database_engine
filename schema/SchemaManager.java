package schema;

import java.util.concurrent.ConcurrentHashMap;

public class SchemaManager {
    ConcurrentHashMap<String,String> schema = new ConcurrentHashMap<>();
    private static final SchemaManager INSTANCE = new SchemaManager();

    /*
    * To prevent from creating new SchemaManager.
    * */
    private SchemaManager(){}

    public static SchemaManager getInstance() {
        return INSTANCE;
    }
    public ConcurrentHashMap<String,String> getSchema(){
        return schema;
    }
}
