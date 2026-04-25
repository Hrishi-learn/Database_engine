package server;

import crash_recovery.WAL;
import scheduler.DiskWriteScheduler;
import schema.SchemaManager;
import storage_engine.KeyValueStore;
import transactions.TransactionManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    public static void main(String[]args) throws IOException {
        SchemaManager.initialize();
        WAL.initialize();
        KeyValueStore.initialize();
        TransactionManager.initialise();
        DiskWriteScheduler.initialize();

        WAL wal = WAL.getInstance();

        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        wal.replay(keyValueStore.getCache(),keyValueStore.getSchema(),keyValueStore.getIndex());


        DiskWriteScheduler diskWriteScheduler = DiskWriteScheduler.getInstance();
        diskWriteScheduler.schedule(keyValueStore.getCache(),keyValueStore.getSchema());

        Executor executor = Executors.newFixedThreadPool(10);

        // shutdown scheduler cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(diskWriteScheduler::shutdown));

        try(ServerSocket serverSocket = new ServerSocket(8080);){
            while(true){
                Socket client = serverSocket.accept();
                Session session = new Session();
                ClientHandler clientHandler= new ClientHandler(client,session);

                executor.execute(clientHandler);
            }
        }catch (IOException e){
            System.out.println(e.getMessage());
        }


    }
}
