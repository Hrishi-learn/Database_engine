import crash_recovery.WAL;
import scheduler.DiskWriteScheduler;
import storage_engine.KeyValueStore;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class Main {
    public static void main(String[]args) throws FileNotFoundException {
        String filepath = "D:\\test";
        Scanner scanner = new Scanner(System.in);

        KeyValueStore keyValueStore = new KeyValueStore(filepath);
        WAL.replay(keyValueStore.getCache(),keyValueStore.getSchema(),keyValueStore.getIndex());

        DiskWriteScheduler diskWriteScheduler = new DiskWriteScheduler();
        diskWriteScheduler.schedule(keyValueStore.getCache(),filepath,keyValueStore.getSchema());

        while (true){
//            System.out.print(">");
            String line = scanner.nextLine();
            if(line.equalsIgnoreCase("exit")){
                break;
            }
            String[] parts = line.split(" ");
            if(parts[0].equalsIgnoreCase(CONSTANTS.CREATE)){
                if(parts[1].equalsIgnoreCase("table")){
                    keyValueStore.createTable(line);
                }else if(parts[1].equalsIgnoreCase("index")){
                    keyValueStore.createIndex(line);
                }
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.INSERT)){
                keyValueStore.put(line);
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.UPDATE)){
                keyValueStore.update(line);
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.DELETE)){
                keyValueStore.delete(line);
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.SELECT)){
                HashMap<String,Integer>queryStrings = new HashMap<>();
                for(String part:parts){
                    queryStrings.put(part,1);
                }
                if(queryStrings.containsKey("where")){
                    if(queryStrings.containsKey("id")){
                        keyValueStore.selectByRowId(line);
                    }
                }else{
                    keyValueStore.selectAllRows(line);
                }
            }

        }
        diskWriteScheduler.shutdown();
    }
}
