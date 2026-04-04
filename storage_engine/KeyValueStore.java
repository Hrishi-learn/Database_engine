package storage_engine;

import crash_recovery.WAL;
import exceptions.InvalidInputException;
import exceptions.TableNotFoundException;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {

    private final String filePath;
    ConcurrentHashMap<String,String> cache;
    private WAL wal;
    ConcurrentHashMap<String,Integer>rowCounter;
    ConcurrentHashMap<String,String>schema;

    public KeyValueStore(String filePath){
        cache = new ConcurrentHashMap<>();
        rowCounter = new ConcurrentHashMap<>();
        schema = new ConcurrentHashMap<>();
        this.filePath = filePath;
        wal = new WAL();
    }
    public void put(String rowToBeInserted) throws FileNotFoundException {
        // rowToBeInserted -> insert name:hrishi age:24 sex:male tableName
        // parts -> [insert, name:hrishi, age:24, sex:male, tableName]
        String[] parts = rowToBeInserted.split(" ");
        int n = parts.length;
        String tableName = parts[n-1];
        if(!schema.containsKey(tableName)){
            throw new TableNotFoundException("No table found: "+tableName);
        }
        // (tableName, col_name1,col_name2,...)
        String currSchema = schema.get(tableName);
        //[col_name1,col_name2,....]
        String []colNames = currSchema.split(",");
        int numberOfCol = colNames.length;
        if(numberOfCol!=n-2){
            throw new InvalidInputException("Number of columns in schema and input do not match");
        }
        List<String>keys = new ArrayList<>();
        List<String>values = new ArrayList<>();

        for(int i=1;i<n-1;i++){
            String[] pair = parts[i].split(":");
            keys.add(pair[0]);
            values.add(pair[1]);
        }

        for(int i=0;i<numberOfCol;i++){
            if(!keys.get(i).equalsIgnoreCase(colNames[i])){
                throw new InvalidInputException("Column names do not match with schema");
            }
        }

        // cache -> (tablename:row_id:key,value)
        int maxRowId = 0;
        for (Map.Entry<String, ?> entry : cache.entrySet()) {
            String key = entry.getKey();
            String[] keyParts = key.split(":");

            if (keyParts[0].equalsIgnoreCase(tableName)) {
                int rowId = Integer.parseInt(keyParts[1]);
                maxRowId = Math.max(maxRowId, rowId);
            }
        }
        rowCounter.put(tableName,maxRowId+1);
        wal.append(keys,values,tableName,rowCounter);

        for(int i=0;i<numberOfCol;i++){
            String keyToPut = tableName+":"+String.valueOf(maxRowId+1)+":"+keys.get(i);
            cache.put(keyToPut,values.get(i));
        }
    }

    public void delete(String key) throws FileNotFoundException {
        //delete from table where id = ?
        String []parts = key.split(" ");
        String tableName = parts[2];
        String row_id = parts[6];
        List<String>keys = new ArrayList<>();
        List<String>values = new ArrayList<>();

        if(!schema.containsKey(tableName)){
            throw new InvalidInputException("Table doesn't exists");
        }

        // (tableName:row_id:key,value)
        for(Map.Entry<String,String>entry:cache.entrySet()){
            String cacheKey = entry.getKey();

            String []cacheKeyParts = cacheKey.split(":");
            if(tableName.equalsIgnoreCase(cacheKeyParts[0]) && row_id.equalsIgnoreCase(cacheKeyParts[1])){
                System.out.println(cacheKeyParts[2]);
                keys.add(cacheKeyParts[2]);
                values.add("_DELETED_");
                cache.replace(cacheKey,"_DELETED_");
            }
        }
        wal.append(keys,values,tableName, Integer.parseInt(row_id));
    }

    public void update(String key) throws FileNotFoundException {
        // update table set key = ? where id = ?
        String []parts = key.split(" ");
        String tableName = parts[1];
        String column = parts[3];
        String columnValue = parts[5];
        String row_id = parts[9];

        if(!checkColumnExist(tableName,column)){
            throw new InvalidInputException("Invalid column or table name");
        }

        cache.replace(tableName+":"+row_id+":"+column,columnValue);
        List<String>keys = new ArrayList<>();
        List<String>values = new ArrayList<>();
        keys.add(column);
        values.add(columnValue);
        wal.append(keys,values,tableName,Integer.parseInt(row_id));
    }

    public void select(String key){

    }
    public void createTable(String key) throws FileNotFoundException {
        String []parts = key.split(" ");
        String tableName = parts[2];
        if(schema.containsKey(tableName)){
            throw new InvalidInputException("Table already exists");
        }
        StringBuilder columns = new StringBuilder("");
        int partsLength = parts.length;
        for(int i=3;i<partsLength-1;i++){
            columns.append(parts[i]);
            columns.append(",");
        }
        columns.append(parts[partsLength-1]);
        wal.append(tableName, columns.toString());
        schema.put(tableName, columns.toString());
    }
    private boolean checkColumnExist(String tableName,String columnName){
        if(!schema.containsKey(tableName))return false;
        String columns = schema.get(tableName);
        String []parts = columns.split(",");

        return Arrays.stream(parts).anyMatch(s->s.equalsIgnoreCase(columnName));
    }

    public ConcurrentHashMap<String,String>getCache(){return cache;}
    public ConcurrentHashMap<String, Integer> getRowCounter() { return rowCounter;}
    public ConcurrentHashMap<String, String> getSchema() {return schema;}
}
