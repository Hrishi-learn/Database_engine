package scheduler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
        service.scheduleAtFixedRate(()->flush(cache,filepath),0,50, TimeUnit.SECONDS);
    }
    private void flush(ConcurrentHashMap<String,String>cache,String filepath){
        cache.forEach((key,value)->{
            try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath,true))){
                bufferedWriter.write(key+":"+value);
                bufferedWriter.newLine();
            }catch (IOException e){
                e.printStackTrace();
            }
        });
        System.out.println("Data flushed");
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
