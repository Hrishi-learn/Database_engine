import crash_recovery.WAL;
import scheduler.DiskWriteScheduler;
import storage_engine.KeyValueStore;

import java.util.Scanner;

public class Main {
    public static void main(String[]args){
        String filepath = "D:\\test";
        Scanner scanner = new Scanner(System.in);

        KeyValueStore keyValueStore = new KeyValueStore(filepath);
        WAL.replay(keyValueStore.getCache());

        DiskWriteScheduler diskWriteScheduler = new DiskWriteScheduler();
        diskWriteScheduler.schedule(keyValueStore.getCache(),filepath);

        while (true){
//            System.out.print(">");
            String line = scanner.nextLine();
            if(line.equalsIgnoreCase("exit")){
                break;
            }
            String[] parts = line.split(" ");

            if(parts[0].equalsIgnoreCase(CONSTANTS.INSERT) || parts[0].equalsIgnoreCase(CONSTANTS.UPDATE)){
                keyValueStore.put(parts[1],parts[2]);
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.GET)){
                System.out.println(keyValueStore.get(parts[1]));
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.DELETE)){
                keyValueStore.delete(parts[1]);
            }
        }
        diskWriteScheduler.shutdown();
    }
}
