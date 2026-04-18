import command_parser.Command;
import command_parser.SimpleParser;
import crash_recovery.WAL;
import scheduler.DiskWriteScheduler;
import schema.SchemaManager;
import storage_engine.KeyValueStore;
import transactions.TransactionManager;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[]args) throws FileNotFoundException {
        String filepath = "D:\\test";
        Scanner scanner = new Scanner(System.in);
        boolean inTransaction = false;
        AtomicInteger transactionId = new AtomicInteger(0);

        SchemaManager schemaManager = SchemaManager.getInstance();
        WAL wal = WAL.getInstance();

        KeyValueStore keyValueStore = new KeyValueStore(filepath,schemaManager.getSchema(),wal);
        wal.replay(keyValueStore.getCache(),keyValueStore.getSchema(),keyValueStore.getIndex());

        TransactionManager transactionManager = new TransactionManager(schemaManager,keyValueStore,wal);

        DiskWriteScheduler diskWriteScheduler = new DiskWriteScheduler();
        diskWriteScheduler.schedule(keyValueStore.getCache(),filepath,keyValueStore.getSchema());

        while (true){
//            System.out.print(">");
            String line = scanner.nextLine();
            if(line.equalsIgnoreCase("exit")){
                break;
            }
            String[] parts = line.split(" ");
            Command command = SimpleParser.parser(line);

            if(parts[0].equalsIgnoreCase(CONSTANTS.CREATE)){
                if(parts[1].equalsIgnoreCase("table")){
                    keyValueStore.createTable(command);
                    System.out.println("Table created");
                }else if(parts[1].equalsIgnoreCase("index")){
                    keyValueStore.createIndex(command);
                    System.out.println("Index created");
                }
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.INSERT)){
                if(!inTransaction){
                    keyValueStore.put(command);
                }else{
                    transactionManager.insert(transactionId.toString(),command);
                }
                System.out.println("Inserted the values successfully");
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.UPDATE)){
                if(!inTransaction){
                    keyValueStore.update(command);
                }else{
                    transactionManager.update(transactionId.toString(),command);
                }
                System.out.println("column updated");
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.DELETE)){
                if(!inTransaction){
                    keyValueStore.delete(command);
                }else{
                    transactionManager.delete(transactionId.toString(),command);
                }
                System.out.println("row deleted");
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.SELECT)){
                HashMap<String,Integer>queryStrings = new HashMap<>();
                for(String part:parts){
                    queryStrings.put(part,1);
                }
                if(queryStrings.containsKey("where")){
                    if(queryStrings.containsKey("id")){
                        if(!inTransaction){
                            keyValueStore.selectByRowId(command);
                        }else{
                            transactionManager.selectByRowId(transactionId.toString(),command);
                        }
                    }else{
                        if(!inTransaction){
                            keyValueStore.selectRowByColumn(command);
                        }else{
                            transactionManager.selectRowByColumn(transactionId.toString(),command);
                        }
                    }
                }else{
                    if(!inTransaction){
                        keyValueStore.selectAllRows(command);
                    }else{
                        transactionManager.selectAllRows(transactionId.toString(),command);
                    }
                }
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.BEGIN)){
                inTransaction = true;
                transactionManager.startTransaction(transactionId.toString());
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.COMMIT)){
                transactionManager.commit(transactionId.toString());
                inTransaction = false;
                transactionId.incrementAndGet();
                System.out.println("changes committed");
            }else if(parts[0].equalsIgnoreCase(CONSTANTS.ROLLBACK)){
                transactionManager.rollback(transactionId.toString());
                inTransaction = false;
                transactionId.incrementAndGet();
                System.out.println("Rollbacked changes");
            }

        }
        diskWriteScheduler.shutdown();
    }
}
