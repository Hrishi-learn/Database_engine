package storage_engine;

import command_parser.Command;
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
    public void put(Command command) throws FileNotFoundException {
        // rowToBeInserted -> insert name:hrishi age:24 sex:male tableName
        // parts -> [insert, name:hrishi, age:24, sex:male, tableName]
        String tableName = command.getTable();
        List<String>columnList = command.getColumns();
        HashMap<String,String>columnValueMap = command.getColumnValueMap();

        if(!schema.containsKey(tableName)){
            throw new TableNotFoundException("No table found: "+tableName);
        }
        // (tableName, col_name1,col_name2,...)
        String currSchema = schema.get(tableName);
        //[col_name1,col_name2,....]
        String []colNames = currSchema.split(",");
        int numberOfCol = colNames.length;
        if(numberOfCol!=columnList.size()){
            throw new InvalidInputException("Number of columns in schema and input do not match");
        }
        for(int i=0;i<numberOfCol;i++){
            if(!columnList.get(i).equalsIgnoreCase(colNames[i])){
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
        wal.append(columnValueMap,tableName,rowCounter);
        int rowId = rowCounter.get(tableName);

        /*
        * updating the index table for new inserts
        * */
        for(Map.Entry<String,String>entry:columnValueMap.entrySet()){
            String column = entry.getKey();
            String value = entry.getValue();
            if(index.containsKey(tableName) && index.get(tableName).containsKey(column)){
                TreeMap<String,HashSet<Integer>>columnIndex = index.get(tableName).get(column);
                columnIndex.computeIfAbsent(value,val -> new HashSet<>()).add(rowId);
            }
        }
        cache.computeIfAbsent(tableName,val -> new HashMap<>()).putIfAbsent(Integer.toString(rowId),columnValueMap);
    }

    public void delete(Command commmand) throws FileNotFoundException {
        //delete from table where id = ?
        String tableName = commmand.getTable();
        String row_id = commmand.getRowId();
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

    public void update(Command command) throws FileNotFoundException {
        // update table set key = ? where id = ?
        String tableName = command.getTable();
        String column = command.getColumns().get(0);
        String columnValue = command.getColumnValueMap().get(column);
        String row_id = command.getRowId();

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

    public void selectByRowId(Command command){
        /**
            select * from users where id = 1
            select name,age from users where id = 1
         */
        String table =  command.getTable();
        if(!schema.containsKey(table)){
            throw new InvalidInputException("table doesn't exist");
        }
        String []columns = schema.get(table).split(",");
        String row_id = command.getRowId();
        // table:row_id:column
        HashMap<String,String>columnValues = new HashMap<>();
        StringBuilder result = new StringBuilder();
        for(String column:columns){
            String columnValue = cache.get(table).get(row_id).get(column);
            columnValues.put(column,columnValue);
            result.append(columnValue);
            result.append(" ");
        }
        if(!command.isEntireRow()){
            result.setLength(0);
            List<String>columnsToQuery = command.getColumns();
            for(String column:columnsToQuery){
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
    public void selectAllRows(Command command){
         String table = command.getTable();
         if(!schema.containsKey(table)){
             throw new InvalidInputException("Table doesn't exists");
         }
         List<String>columns = new ArrayList<>();
         if(command.isEntireRow()){
             String schemaValue = schema.get(table);
             columns = List.of(schemaValue.split(","));
         }else{
             columns = command.getColumns();
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
    public void selectRowByColumn(Command command){

        String table = command.getTable();
        String columnName = command.getColumns().get(0);
        String columnValue = command.getColumnValueMap().get(columnName);

        List<String>totalColumnsToQuery = command.getColumns();
        String []totalColumnsInARow = schema.get(table).split(",");

        if(!schema.containsKey(table)){
            throw new InvalidInputException("The table do not exists");
        }

        if(command.isEntireRow()){
            totalColumnsToQuery = List.of(totalColumnsInARow);
        }

        if(Arrays.stream(totalColumnsInARow).noneMatch(column->column.equalsIgnoreCase(columnName))){
            throw new InvalidInputException("The column do not exists");
        }

        if(index.containsKey(table) && index.get(table).containsKey(columnName)){
            HashSet<Integer>rowIds = index.get(table).get(columnName).get(columnValue);
            for(Integer row:rowIds){
                HashMap<String,String>rowColumnValues = cache.get(table).get(Integer.toString(row));
                if(!rowColumnValues.get(columnName).equals(columnValue)){
                    continue;
                }

                for(String column:totalColumnsToQuery){
                    System.out.print(rowColumnValues.get(column)+" ");
                }
                System.out.println();
            }
        }else{
            HashMap<String,HashMap<String,String>>rowColumnMap = cache.get(table);
            List<String>rowIds = new ArrayList<>();
            for(Map.Entry<String,HashMap<String,String>>entry:rowColumnMap.entrySet()){
                String rowId = entry.getKey();
                HashMap<String,String>columnValueMap = entry.getValue();
                if(columnValueMap.get(columnName).equals(columnValue)){
                    rowIds.add(rowId);
                }
            }
            for(String rowId:rowIds){
                HashMap<String,String>rowColumnValues = cache.get(table).get(rowId);
                for(String column:totalColumnsToQuery){
                    System.out.print(rowColumnValues.get(column)+" ");
                }
                System.out.println();
            }
        }

    }

    public void createTable(Command command) throws FileNotFoundException {
        String tableName = command.getTable();
        if(schema.containsKey(tableName)){
            throw new InvalidInputException("Table already exists");
        }
        StringBuilder columns = new StringBuilder("");
        List<String>columnsFromQuery = command.getColumns();
        int totalNoOfColumns = columnsFromQuery.size();
        for(int i=0;i<totalNoOfColumns-1;i++){
            columns.append(columnsFromQuery.get(i));
            columns.append(",");
        }
        columns.append(columnsFromQuery.get(totalNoOfColumns-1));
        wal.append("schema",tableName, columns.toString());
        schema.put(tableName, columns.toString());
    }

    public void createIndex(Command command) throws FileNotFoundException {

        String table = command.getTable();
        String columnName = command.getColumns().get(0);

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

        HashMap<String,HashMap<String,String>>rowColumnPairs = cache.computeIfAbsent(table,x->new HashMap<>());
        for(Map.Entry<String,HashMap<String,String>>entry:rowColumnPairs.entrySet()){
            String rowId = entry.getKey();
            HashMap<String,String>columnValuePairs = entry.getValue();
            for(Map.Entry<String,String>nestedEntry:columnValuePairs.entrySet()){
                String column = nestedEntry.getKey();
                String value = nestedEntry.getValue();
                if(column.equalsIgnoreCase(columnName)){
                    index.get(table).get(columnName).computeIfAbsent(value,x-> new HashSet<>()).add(Integer.parseInt(rowId));
                }
            }
        }
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
