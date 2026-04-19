package server;

import command_parser.CONSTANTS;
import command_parser.Command;
import command_parser.CommandType;
import command_parser.SimpleParser;
import crash_recovery.WAL;
import storage_engine.KeyValueStore;
import transactions.TransactionManager;

import java.io.FileNotFoundException;
import java.util.HashMap;

public class Router {

    public static String route(String clientInput,Session session) throws FileNotFoundException {

        String []parts = clientInput.split(" ");
        Command command = SimpleParser.parser(clientInput);

        StringBuilder result = new StringBuilder();

        WAL wal = WAL.getInstance();

        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        wal.replay(keyValueStore.getCache(),keyValueStore.getSchema(),keyValueStore.getIndex());

        TransactionManager transactionManager = TransactionManager.getInstance();

        if(parts[0].equalsIgnoreCase(CONSTANTS.CREATE)){
            if(parts[1].equalsIgnoreCase("table")){
                keyValueStore.createTable(command);
                result.append("Table created\nEND");
            }else if(parts[1].equalsIgnoreCase("index")){
                keyValueStore.createIndex(command);
                result.append("Index created\nEND");
            }
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.INSERT)){
            if(!session.isInTransaction()){
                keyValueStore.put(command);
            }else{
                transactionManager.insert(session.getTransactionId(), command);
            }
            result.append("Inserted the values successfully\nEND");
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.UPDATE)){
            if(!session.isInTransaction()){
                keyValueStore.update(command);
            }else{
                transactionManager.update(session.getTransactionId(), command);
            }
            result.append("column updated\nEND");
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.DELETE)){
            if(!session.isInTransaction()){
                keyValueStore.delete(command);
            }else{
                transactionManager.delete(session.getTransactionId(), command);
            }
            result.append("row deleted\nEND");
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.SELECT)){
            HashMap<String,Integer> queryStrings = new HashMap<>();
            for(String part:parts){
                queryStrings.put(part,1);
            }
            if(queryStrings.containsKey("where")){
                if(queryStrings.containsKey("id")){
                    if(!session.isInTransaction()){
                        String queryResult = keyValueStore.selectByRowId(command);
                        result.append(queryResult);
                    }else{
                        String queryResult = transactionManager.selectByRowId(session.getTransactionId(), command);
                        result.append(queryResult);
                    }
                }else{
                    if(!session.isInTransaction()){
                        String queryResult = keyValueStore.selectRowByColumn(command);
                        result.append(queryResult);
                    }else{
                        String queryResult = transactionManager.selectRowByColumn(session.getTransactionId(), command);
                        result.append(queryResult);
                    }
                }
            }else{
                if(!session.isInTransaction()){
                    String queryResult = keyValueStore.selectAllRows(command);
                    result.append(queryResult);
                }else{
                    String queryResult = transactionManager.selectAllRows(session.getTransactionId(), command);
                    result.append(queryResult);
                }
            }
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.BEGIN)){
            session.setInTransaction(true);
            session.setTransactionId(transactionManager.startTransaction());
            result.append("Transaction begin\nEND");
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.COMMIT)){
            transactionManager.commit(session.getTransactionId());
            session.setInTransaction(false);
            result.append("changes committed\nEND");
        }else if(parts[0].equalsIgnoreCase(CONSTANTS.ROLLBACK)){
            transactionManager.rollback(session.getTransactionId());
            session.setInTransaction(false);
            result.append("Rollbacked changes\nEND");
        }

        return result.toString();
    }
}
