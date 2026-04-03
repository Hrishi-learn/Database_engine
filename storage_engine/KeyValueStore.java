package storage_engine;

import crash_recovery.WAL;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {

    private final String filePath;
    ConcurrentHashMap<String,String> cache;
    private WAL wal;

    public KeyValueStore(String filePath){
        cache = new ConcurrentHashMap<>();
        this.filePath = filePath;
        wal = new WAL();
    }
    public void put(String key,String value) throws FileNotFoundException {
        wal.append(key, value);
        cache.put(key,value);
    }
//    public void flush(String key,String value){
//        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))){
//            bufferedWriter.write(key+":"+value);
//            bufferedWriter.newLine();
//        }catch (IOException e){
//            e.printStackTrace();
//        }
//    }
    public String get(String key){
        return cache.getOrDefault(key,"");
    }
    public String getFromDisk(String key){
        String value="";
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))){
            String line;
            while((line= bufferedReader.readLine())!=null){
                int pos = line.indexOf(':');
                if(pos==-1)return "";
                String lineKey = line.substring(0,pos);
                String lineValue = line.substring(pos+1);
                if(key.equals(lineKey)){
                    value=lineValue;
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return value;
    }
    public void delete(String key) throws FileNotFoundException {
        put(key,"_DELETED_");
        System.out.println("Key deleted");
    }

    public ConcurrentHashMap<String,String>getCache(){
        return cache;
    }
}
