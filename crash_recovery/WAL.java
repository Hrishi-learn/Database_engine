package crash_recovery;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WAL {
    public final static String wal_log_path = "D:\\wal.log";

     public WAL(){
        try {
            File file = new File(wal_log_path);
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void append(HashMap<String,String>columnValuePairs, String table, ConcurrentHashMap<String,Integer>rowCounter) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(wal_log_path,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos));

        int rowid = rowCounter.get(table);

        try{
            for(Map.Entry<String,String>entry:columnValuePairs.entrySet()){
                String key = entry.getKey();
                String value = entry.getValue();
                bufferedWriter.write(table+":"+rowid+":"+key+"="+value);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(List<String>keys, List<String>values, String table, int rowid) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(wal_log_path,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos));

        try{
            for(int i=0;i<keys.size();i++){
                bufferedWriter.write(table+":"+rowid+":"+keys.get(i)+"="+values.get(i));
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(String functionality,String tableName,String columns) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(wal_log_path, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos));

        try {
            bufferedWriter.write(functionality + ":" + tableName + ":" + columns);
            bufferedWriter.newLine();
            bufferedWriter.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void replay(HashMap<String,HashMap<String,HashMap<String,String>>> cache, ConcurrentHashMap<String,String>schema, HashMap<String,HashMap<String, TreeMap<String, HashSet<Integer>>>> index){
         List<String>rowColumnData = new ArrayList<>();
         try(BufferedReader bufferedReader = new BufferedReader(new FileReader(wal_log_path))){
            String line;
            while((line= bufferedReader.readLine())!=null){
                String[]parts = line.split(":");
                if(parts[0].equalsIgnoreCase("schema")){
                    schema.put(parts[1],parts[2]);
                }
                else if(parts[0].equalsIgnoreCase("index")){
                    HashMap<String,TreeMap<String,HashSet<Integer>>>columnIndex = new HashMap<>();
                    columnIndex.put(parts[2],new TreeMap<>());
                    index.put(parts[1],columnIndex);
                }
                else{
                    rowColumnData.add(line);
                    parts = line.split("=");
                    String[] tokens = parts[0].split(":");

                    cache.computeIfAbsent(tokens[0],table-> new HashMap<>())
                            .computeIfAbsent(tokens[1],row-> new HashMap<>())
                            .put(tokens[2],parts[1]);

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

         for(String data:rowColumnData){
             String []tokens = data.split(":");
             String table = tokens[0];
             String rowId = tokens[1];
             String []keyValuePair = tokens[2].split("=");
             String column = keyValuePair[0];
             String value = keyValuePair[1];

             if(index.containsKey(table) && index.get(table).containsKey(column)){
                 TreeMap<String,HashSet<Integer>>columnIndex = index.get(table).get(column);
                 columnIndex.computeIfAbsent(value,val -> new HashSet<>()).add(Integer.parseInt(rowId));
             }
         }

    }
}
