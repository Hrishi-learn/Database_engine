package crash_recovery;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class WAL {
    private final static String wal_log_path = "D:\\wal.log";

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

    public void append(String key,String value){
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(wal_log_path,true))){
            bufferedWriter.write(key+":"+value);
            bufferedWriter.newLine();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void replay(ConcurrentHashMap<String,String> cache){
        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(wal_log_path))){
            String line;
            while((line= bufferedReader.readLine())!=null){
                int pos = line.indexOf(':');
                String lineKey = line.substring(0,pos);
                String lineValue = line.substring(pos+1);
                cache.put(lineKey,lineValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
