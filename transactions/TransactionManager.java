package transactions;

import com.sun.source.tree.Tree;
import command_parser.Command;
import crash_recovery.WAL;
import exceptions.InvalidInputException;
import exceptions.TableNotFoundException;
import schema.SchemaManager;
import storage_engine.KeyValueStore;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionManager {
    /*
    * uncommittedCache - {transaction_id, {table, row_id, {column,value}}}
    * uncommittedIndex - {transaction_id, {table, column, {values,{row1,row2,..}}}
    * */

    HashMap<String,HashMap<String,HashMap<String,HashMap<String,String>>>>uncommittedCacheMap;
    HashMap<String,HashMap<String,HashMap<String, TreeMap<String, HashSet<Integer>>>>>uncommittedIndexMap;
    ConcurrentHashMap<String,String>schema;
    ConcurrentHashMap<String, AtomicInteger>rowCounter;
    HashMap<String,HashMap<String,HashMap<String,String>>>cache;
    HashMap<String,HashMap<String,TreeMap<String,HashSet<Integer>>>>index;

    WAL wal;

    public TransactionManager(SchemaManager schemaManager, KeyValueStore kv, WAL wal){
        uncommittedCacheMap = new HashMap<>();
        uncommittedIndexMap = new HashMap<>();
        schema = schemaManager.getSchema();
        rowCounter = kv.getRowCounter();
        cache = kv.getCache();
        index = kv.getIndex();
        this.wal = wal;
    }

    public void startTransaction(String transaction_id){
        HashMap<String,HashMap<String,HashMap<String,String>>>uncCommittedCache = new HashMap<>();
        HashMap<String,HashMap<String, TreeMap<String, HashSet<Integer>>>>uncommittedIndex = new HashMap<>();
        uncommittedCacheMap.put(transaction_id,uncCommittedCache);
        uncommittedIndexMap.put(transaction_id,uncommittedIndex);
    }
    public void insert(String transaction_id, Command command){
        String tableName = command.getTable();
        List<String> columnList = command.getColumns();
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
        rowCounter.computeIfAbsent(tableName, k -> {
            // this block runs atomically only if key doesn't exist
            HashMap<String, HashMap<String, String>> columns = cache.get(tableName);
            int maxRowId = 0;
            if (columns != null) {
                for (Map.Entry<String, ?> entry : columns.entrySet()) {
                    maxRowId = Math.max(maxRowId, Integer.parseInt(entry.getKey()));
                }
            }
            return new AtomicInteger(maxRowId);
        }).incrementAndGet();
        AtomicInteger rowId = rowCounter.get(tableName);
        /*
         * updating the index table for new inserts
         * */
        HashMap<String, HashMap<String, TreeMap<String, HashSet<Integer>>>> uncommittedIndex = uncommittedIndexMap.get(transaction_id);
        for(Map.Entry<String,String>entry:columnValueMap.entrySet()){
            String column = entry.getKey();
            String value = entry.getValue();
            if(index.containsKey(tableName) && index.get(tableName).containsKey(column)){
                uncommittedIndex.computeIfAbsent(tableName, val->new HashMap<>()).computeIfAbsent(column, val-> new TreeMap<>())
                        .computeIfAbsent(value,val->new HashSet<>()).add(rowId.intValue());
            }
        }
        HashMap<String,HashMap<String,HashMap<String,String>>>uncommittedCache = uncommittedCacheMap.get(transaction_id);
        uncommittedCache.computeIfAbsent(tableName,val -> new HashMap<>()).putIfAbsent(Integer.toString(rowId.intValue()),columnValueMap);
    }
    public void update(String transaction_id,Command command){
        String tableName = command.getTable();
        String column = command.getColumns().get(0);
        String columnValue = command.getColumnValueMap().get(column);
        String row_id = command.getRowId();

        if(!checkColumnExist(tableName,column)){
            throw new InvalidInputException("Invalid column or table name");
        }
        HashMap<String,HashMap<String,TreeMap<String,HashSet<Integer>>>>uncommittedIndex = uncommittedIndexMap.get(transaction_id);
        if(uncommittedIndex.containsKey(tableName) && uncommittedIndex.get(tableName).containsKey(column)){
            TreeMap<String,HashSet<Integer>>columnValues = uncommittedIndex.get(tableName).get(column);
            String oldValue = cache.get(tableName).get(row_id).get(column);
            HashSet<Integer>oldValueRowIds = columnValues.get(oldValue);
            oldValueRowIds.remove(Integer.parseInt(row_id));

            uncommittedIndex.get(tableName).get(column).computeIfAbsent(columnValue, val-> new HashSet<>()).add(Integer.parseInt(row_id));
        }else if((index.containsKey(tableName) && index.get(tableName).containsKey(column))){
            uncommittedIndex.computeIfAbsent(tableName, val->new HashMap<>()).computeIfAbsent(column, val-> new TreeMap<>())
                    .computeIfAbsent(columnValue,val->new HashSet<>()).add(Integer.parseInt(row_id));
        }
        HashMap<String,HashMap<String,HashMap<String,String>>>uncommittedCache = uncommittedCacheMap.get(transaction_id);
        uncommittedCache.computeIfAbsent(tableName,val -> new HashMap<>()).computeIfAbsent(row_id,val->new HashMap<>()).put(column,columnValue);
    }
    public void delete(String transaction_id,Command command){
        //delete from table where id = ?
        String tableName = command.getTable();
        String row_id = command.getRowId();

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
            HashMap<String,HashMap<String,HashMap<String,String>>>uncommittedCache = uncommittedCacheMap.get(transaction_id);
            uncommittedCache.computeIfAbsent(tableName,val -> new HashMap<>()).computeIfAbsent(row_id,val->new HashMap<>()).put(column,"__DELETED__");
        }
    }
    public void selectByRowId(String transaction_id,Command command){
        /**
         select * from users where id = 1
         select name,age from users where id = 1
         */
        String table =  command.getTable();
        String row_id = command.getRowId();
        // start with committed data
        HashMap<String, String> mergedResult = new HashMap<>(cache.get(table).get(row_id));
        HashMap<String, HashMap<String,HashMap<String,String>>>intermediate = uncommittedCacheMap.get(transaction_id);

        if (intermediate.containsKey(table) &&
                intermediate.get(table).containsKey(row_id)) {
            mergedResult.putAll(intermediate.get(table).get(row_id));  // intermediate wins
        }

        if(!schema.containsKey(table)){
            throw new InvalidInputException("table doesn't exist");
        }
        String []columns = schema.get(table).split(",");

        // table:row_id:column
        HashMap<String,String>columnValues = new HashMap<>();
        StringBuilder result = new StringBuilder();
        for(String column:columns){
            String columnValue = mergedResult.get(column);
            columnValues.put(column,columnValue);
            result.append(columnValue);
            result.append(" ");
        }
        if(!command.isEntireRow()){
            result.setLength(0);
            List<String>columnsToQuery = command.getColumns();
            for(String column:columnsToQuery){
                if(!mergedResult.containsKey(column)){
                    throw new InvalidInputException("row id or column doesn't exist");
                }
                String columnValue = mergedResult.get(column);
                result.append(columnValue);
                result.append(" ");
            }
        }
        System.out.println(result.toString());
    }
    public void selectAllRows(String transaction_id,Command command){
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

        HashMap<String, HashMap<String, String>> result = mergeIntermediateAndCache(table,transaction_id);


        for(Map.Entry<String,HashMap<String,String>>rows: result.entrySet()){
            String rowId = rows.getKey();
            HashMap<String,String>rowColumnPairs = rows.getValue();
            System.out.print(rowId+" ");
            for(String column:columns){
                System.out.print(rowColumnPairs.get(column)+" ");
            }
            System.out.println();
        }

    }
    public void selectRowByColumn(String transaction_id,Command command){

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
        HashSet<Integer>rowIds = new HashSet<>();
        if(index.containsKey(table) && index.get(table).containsKey(columnName) && index.get(table).get(columnName).get(columnValue)!=null){
            rowIds = index.get(table).get(columnName).get(columnValue);
        }
        HashMap<String,HashMap<String,String>>result = mergeIntermediateAndCache(table,transaction_id);
        for(Map.Entry<String,HashMap<String,String>>entry:result.entrySet()){
            String rowId = entry.getKey();
            HashMap<String,String>columnValueMap = entry.getValue();
            if(columnValueMap.get(columnName).equals(columnValue)){
                rowIds.add(Integer.parseInt(rowId));
            }
        }
        for(Integer rowId:rowIds){
            HashMap<String,String>columnValues = result.get(rowId.toString());
            if(!columnValues.get(columnName).equals(columnValue))continue;
            for(String column:totalColumnsToQuery){
                System.out.print(columnValues.get(column)+" ");
            }
            System.out.println();
        }

    }

    public HashMap<String,HashMap<String,String>> mergeIntermediateAndCache(String table,String transaction_id){
        HashMap<String, HashMap<String,HashMap<String,String>>>intermediate = uncommittedCacheMap.get(transaction_id);
        // get all rowIds from both dataMap and intermediate
        HashSet<String> allRowIds = new HashSet<>();

        if (cache.containsKey(table)) {
            allRowIds.addAll(cache.get(table).keySet());
        }
        if (intermediate.containsKey(table)) {
            allRowIds.addAll(intermediate.get(table).keySet());
        }

        HashMap<String, HashMap<String, String>> result = new HashMap<>();

        for (String rowId : allRowIds) {
            HashMap<String, String> mergedRow = new HashMap<>();
            if (cache.containsKey(table) &&
                    cache.get(table).containsKey(rowId)) {
                mergedRow.putAll(cache.get(table).get(rowId));
            }
            if (intermediate.containsKey(table) &&
                    intermediate.get(table).containsKey(rowId)) {
                mergedRow.putAll(intermediate.get(table).get(rowId));
            }
            result.put(rowId, mergedRow);
        }
        return result;
    }

    public void commit(String transaction_id) throws FileNotFoundException {
        HashMap<String,HashMap<String,HashMap<String,String>>>uncommittedCache = uncommittedCacheMap.get(transaction_id);
        HashMap<String,HashMap<String,TreeMap<String,HashSet<Integer>>>>uncommittedIndex = uncommittedIndexMap.get(transaction_id);
        wal.append(uncommittedCache);

        for (Map.Entry<String, HashMap<String, TreeMap<String, HashSet<Integer>>>> tableEntry : uncommittedIndex.entrySet()) {
            String tableName = tableEntry.getKey();
            for (Map.Entry<String, TreeMap<String, HashSet<Integer>>> columnEntry : tableEntry.getValue().entrySet()) {
                String columnName = columnEntry.getKey();
                for (Map.Entry<String, HashSet<Integer>> valueEntry : columnEntry.getValue().entrySet()) {
                    String columnValue = valueEntry.getKey();
                    HashSet<Integer> rowIds = valueEntry.getValue();
                    for (Integer rowId : rowIds) {
                        // get OLD value from dataMap before updating
                        if (cache.containsKey(tableName) && cache.get(tableName).containsKey(rowId.toString())) {
                            String oldValue = cache.get(tableName)
                                    .get(rowId.toString())
                                    .get(columnName);

                            if (oldValue != null && index.containsKey(tableName) && index.get(tableName).containsKey(columnName) && index.get(tableName).get(columnName).containsKey(oldValue)) {
                                index.get(tableName).get(columnName).get(oldValue).remove(rowId);  // ←  remove old
                            }
                        }
                    }
                    index
                            .computeIfAbsent(tableName, k -> new HashMap<>())
                            .computeIfAbsent(columnName, k -> new TreeMap<>())
                            .computeIfAbsent(columnValue, k -> new HashSet<>())
                            .addAll(rowIds);
                }
            }
        }

        for(Map.Entry<String,HashMap<String,HashMap<String,String>>>entry1:uncommittedCache.entrySet()){
            String table = entry1.getKey();
            HashMap<String,HashMap<String,String>>rowColumnValues = entry1.getValue();
            for(Map.Entry<String,HashMap<String,String>>entry2:rowColumnValues.entrySet()){
                String rowId = entry2.getKey();
                HashMap<String,String>columnValuePairs = entry2.getValue();
                for(Map.Entry<String,String>entry3:columnValuePairs.entrySet()){
                    String column = entry3.getKey();
                    String value = entry3.getValue();
                    cache.computeIfAbsent(table,val-> new HashMap<>()).computeIfAbsent(rowId,val -> new HashMap<>()).put(column,value);
                }
            }
        }

        uncommittedCacheMap.remove(transaction_id);
        uncommittedIndexMap.remove(transaction_id);

    }
    public void rollback(String transaction_id){
        uncommittedIndexMap.remove(transaction_id);
        uncommittedCacheMap.remove(transaction_id);
    }


    private boolean checkColumnExist(String tableName,String columnName){
        if(!schema.containsKey(tableName))return false;
        String columns = schema.get(tableName);
        String []parts = columns.split(",");

        return Arrays.stream(parts).anyMatch(s->s.equalsIgnoreCase(columnName));
    }
}
