package scheduler;

import crash_recovery.WAL;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DiskWriteScheduler {

    private ScheduledExecutorService service;
    public DiskWriteScheduler(){
        service =  Executors.newScheduledThreadPool(1);
    }
    public void schedule(ConcurrentHashMap<String,String> cache, String filepath){
        service.scheduleAtFixedRate(()->flush(cache,filepath),50,50, TimeUnit.SECONDS);
    }
    private void flush(ConcurrentHashMap<String,String>cache,String filepath){
        /*
          change pending, need to handle the case when app crashes during
          flushing from memory to OS disk.
         */
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath))){
            cache.forEach((key,value)->{
                try {
                    bufferedWriter.write(key+":"+value);
                    bufferedWriter.newLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("Data flushed");

         /*
           WAL compaction, copy the contents to hashmap
           and then again copy the contents to WAL, hashmap do not store
           the duplicate values
          */
        HashMap<String,String>temp = new HashMap<>();
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(WAL.wal_log_path))){
            String line;
            while((line=bufferedReader.readLine())!=null){
                String[] parts = line.split(":");
                temp.put(parts[0],parts[1]);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(WAL.wal_log_path))){
            temp.forEach((key,value)->{
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
