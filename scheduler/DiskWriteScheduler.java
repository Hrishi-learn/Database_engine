package scheduler;

import crash_recovery.WAL;
import schema.SchemaManager;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DiskWriteScheduler {
    private static DiskWriteScheduler INSTANCE;
    private final String filePath = "D:\\test";
    private ScheduledExecutorService service;

    private DiskWriteScheduler(){
        service =  Executors.newScheduledThreadPool(1);
    }

    public static void initialize() {
        if (INSTANCE == null) {
            INSTANCE = new DiskWriteScheduler();
        }
    }
    public static DiskWriteScheduler getInstance() {
        return INSTANCE;
    }

    public void schedule(HashMap<String,HashMap<String,HashMap<String,String>>> cache, ConcurrentHashMap<String,String>schema){
        service.scheduleAtFixedRate(()->flush(cache,schema),300,300, TimeUnit.SECONDS);
    }
    private void flush(HashMap<String,HashMap<String,HashMap<String,String>>>cache,ConcurrentHashMap<String,String>schema){
        /*
          change pending, need to handle the case when app crashes during
          flushing from memory to OS disk.

          also need to handle concurrency because concurrentHashMap provides
          atomicity for single key-value pair but for our case we need concurrency for
          multiple key value pairs,
          users:1:name=hrishi
          users:1:age=24
         */
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))){
            schema.forEach((key,value)->{
                try {
                    bufferedWriter.write("schema"+":"+key+":"+value);
                    bufferedWriter.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            cache.forEach((table,rows)->{
                rows.forEach((row,columns)->{
                    columns.forEach((column,value)->{
                        try{
                            bufferedWriter.write(table+":"+row+":"+column+"="+value);
                            bufferedWriter.newLine();
                        }catch (IOException e){
                            throw new RuntimeException(e);
                        }
                    });
                });
            });
        }catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("Data flushed");

        walCompaction();
    }
    private void walCompaction(){
        /*
           WAL compaction, copy the contents to hashmap
           and then again copy the contents to WAL, hashmap do not store
           the duplicate values
          */
        //schema:user:col_name1,col_name2,col_name3
        //user:row_id:key=value
        HashMap<String,String>tempKeyValues = new HashMap<>();
        HashMap<String,String>schemaMap = new HashMap<>();
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(WAL.wal_log_path))){
            String line;
            while((line=bufferedReader.readLine())!=null){
                String[] colonSeperation = line.split(":");
                if(colonSeperation[0].equalsIgnoreCase("schema")){
                    schemaMap.put(colonSeperation[0]+":"+colonSeperation[1],colonSeperation[2]);
                }
                else{
                    String[] parts = line.split("=");
                    tempKeyValues.put(parts[0],parts[1]);
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(WAL.wal_log_path))){
            tempKeyValues.forEach((key,value)->{
                try {
                    bufferedWriter.write(key+"="+value);
                    bufferedWriter.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            schemaMap.forEach((key,value)->{
                try {
                    bufferedWriter.write(key+":"+value);
                    bufferedWriter.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void shutdown(){
        service.shutdown();
        try {
            service.awaitTermination(5, TimeUnit.SECONDS);
            System.out.println("Flushing to disk finished");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
