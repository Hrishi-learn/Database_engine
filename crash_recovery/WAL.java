package crash_recovery;

import java.io.*;
import java.util.HashMap;
import java.util.List;
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

    public void append(List<String>keys, List<String>values, String table, ConcurrentHashMap<String,Integer>rowCounter) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(wal_log_path,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos));

        int rowid = rowCounter.get(table);

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

    public void append(String tableName,String columns) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(wal_log_path, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos));

        try {
            bufferedWriter.write("schema" + ":" + tableName + ":" + columns);
            bufferedWriter.newLine();
            bufferedWriter.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

        public static void replay(HashMap<String,HashMap<String,HashMap<String,String>>> cache,ConcurrentHashMap<String,String>schema,ConcurrentHashMap<String,Integer>rowCounter){
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(wal_log_path))){
            String line;
            while((line= bufferedReader.readLine())!=null){
                String[]parts = line.split(":");
                if(parts[0].equalsIgnoreCase("schema")){
                    schema.put(parts[1],parts[2]);
                }else{
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
    }
}
