package storage_engine;

import crash_recovery.WAL;
import exceptions.InvalidInputException;
import exceptions.TableNotFoundException;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {

    private final String filePath;
    HashMap<String,HashMap<String,HashMap<String,String>>>cache;
    private WAL wal;
    ConcurrentHashMap<String,Integer>rowCounter;
    ConcurrentHashMap<String,String>schema;
    HashMap<String,HashMap<String,TreeMap<String,HashSet<Integer>>>>index;

    public KeyValueStore(String filePath){
        cache = new HashMap<>();
        rowCounter = new ConcurrentHashMap<>();
        schema = new ConcurrentHashMap<>();
        this.filePath = filePath;
        wal = new WAL();
        index = new HashMap<>();
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

        // cache -> (table,{row_id,{key,value}})
        if(rowCounter.containsKey(tableName)){
            int rowId = rowCounter.get(tableName);
            rowCounter.replace(tableName,rowId+1);
        }else{
            /*
                rowCounter is not persistent we need to
                iterate and find the max row value
            */
            HashMap<String,HashMap<String,String>>columns = cache.get(tableName);
            int maxRowId = 0;
            if(columns!=null){
                for(Map.Entry<String,?>entry:columns.entrySet()){
                    String rowid = entry.getKey();
                    maxRowId = Math.max(maxRowId,Integer.parseInt(rowid));
                }
            }
            rowCounter.put(tableName,maxRowId+1);
        }
        wal.append(keys,values,tableName,rowCounter);
        int rowId = rowCounter.get(tableName);

        HashMap<String,String>columnValues = new HashMap<>();
        for(int i=0;i<numberOfCol;i++){
            columnValues.put(keys.get(i),values.get(i));
            if(index.containsKey(tableName) && index.get(tableName).containsKey(keys.get(i))){
                TreeMap<String,HashSet<Integer>>columnIndex = index.get(tableName).get(keys.get(i));
                columnIndex.computeIfAbsent(values.get(i),val -> new HashSet<>()).add(rowId);
            }
        }
        cache.computeIfAbsent(tableName,val -> new HashMap<>()).putIfAbsent(Integer.toString(rowId),columnValues);
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

        // cache -> (table,{row_id,{key,value}})
        HashMap<String,String>columnValues = cache.get(tableName).get(row_id);
        if(columnValues==null){
            throw new InvalidInputException("Row doesn't exists");
        }
        for(Map.Entry<String, String> entry:columnValues.entrySet()){
            String column = entry.getKey();
            String columnValue = entry.getValue();
            keys.add(column);
            values.add("__DELETED__");
            cache.get(tableName).get(row_id).replace(column,"__DELETED__");
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

        List<String>keys = new ArrayList<>();
        List<String>values = new ArrayList<>();
        keys.add(column);
        values.add(columnValue);
        wal.append(keys,values,tableName,Integer.parseInt(row_id));

        if(index.containsKey(tableName) && index.get(tableName).containsKey(column)){
           TreeMap<String,HashSet<Integer>>columnValues = index.get(tableName).get(column);
           String oldValue = cache.get(tableName).get(row_id).get(column);
           HashSet<Integer>oldValueRowIds = columnValues.get(oldValue);
           oldValueRowIds.remove(Integer.parseInt(row_id));

           index.get(tableName).get(column).computeIfAbsent(columnValue, val-> new HashSet<>()).add(Integer.parseInt(row_id));
        }
        cache.get(tableName).get(row_id).replace(column,columnValue);
    }

    public void selectByRowId(String key){
        String [] tokens = key.split(" ");
        /**
            select * from users where id = 1
            select name,age from users where id = 1
         */
        String table =  tokens[3];
        if(!schema.containsKey(table)){
            throw new InvalidInputException("table doesn't exist");
        }
        String []columns = schema.get(table).split(",");
        String row_id = tokens[7];
        // table:row_id:column
        HashMap<String,String>columnValues = new HashMap<>();
        StringBuilder result =new StringBuilder();
        for(String column:columns){
            String columnValue = cache.get(table).get(row_id).get(column);
            columnValues.put(column,columnValue);
            result.append(columnValue);
            result.append(" ");
        }
        if(!tokens[1].equals("*")){
            String [] parts = tokens[1].split(",");
            result.setLength(0);
            for(String column:parts){
                if(!cache.get(table).get(row_id).containsKey(column)){
                    throw new InvalidInputException("row id or column doesn't exist");
                }
                String columnValue = cache.get(table).get(row_id).get(column);
                result.append(columnValue);
                result.append(" ");
            }
        }
        System.out.println(result.toString());
    }
    public void selectAllRows(String key){
         String []tokens = key.split(" ");
         String table = tokens[3];
         if(!schema.containsKey(table)){
             throw new InvalidInputException("Table doesn't exists");
         }
         List<String>columns = new ArrayList<>();
         if(tokens[1].equals("*")){
             String schemaValue = schema.get(table);
             columns = List.of(schemaValue.split(","));
         }else{
             columns = List.of(tokens[1].split(","));
         }
         HashMap<String,HashMap<String,String>>rowColumns = cache.get(table);

//         for(String column:columns){
//             System.out.print(column + " ");
//         }
//         System.out.println();

         for(Map.Entry<String,HashMap<String,String>>entry:rowColumns.entrySet()){
             String row = entry.getKey();
             HashMap<String,String>colVal = entry.getValue();
             System.out.print(row+" ");
             for(String column:columns){
                 System.out.print(colVal.get(column)+" ");
             }
             System.out.println();
         }

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
        wal.append("schema",tableName, columns.toString());
        schema.put(tableName, columns.toString());
    }

    public void createIndex(String key) throws FileNotFoundException {
        String []tokens = key.split(" ");
        String table = tokens[3];
        String columnName = tokens[4];

        if(!schema.containsKey(table)){
            throw new InvalidInputException("Table doesn't exists");
        }
        String[] columns = schema.get(table).split(",");
        if(Arrays.stream(columns).noneMatch(column->column.equalsIgnoreCase(columnName))){
            throw new InvalidInputException("Column doesn't exists");
        }
        if(index.containsKey(table) && index.get(table).containsKey(columnName)){
            System.out.println("Index already exists");
            return;
        }

        wal.append("index",table,columnName);

        HashMap<String,TreeMap<String,HashSet<Integer>>>columnIndex = new HashMap<>();
        columnIndex.put(columnName,new TreeMap<>());
        index.put(table,columnIndex);
    }

    private boolean checkColumnExist(String tableName,String columnName){
        if(!schema.containsKey(tableName))return false;
        String columns = schema.get(tableName);
        String []parts = columns.split(",");

        return Arrays.stream(parts).anyMatch(s->s.equalsIgnoreCase(columnName));
    }

    public HashMap<String,HashMap<String,HashMap<String,String>>>getCache(){return cache;}
    public ConcurrentHashMap<String, Integer> getRowCounter() { return rowCounter;}
    public ConcurrentHashMap<String, String> getSchema() {return schema;}
    public HashMap<String,HashMap<String,TreeMap<String,HashSet<Integer>>>> getIndex(){ return index;}
}
