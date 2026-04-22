package server;

import command_parser.Command;
import command_parser.SimpleParser;
import transactions.TransactionManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable{
    private final Socket socket;
    private final Session session;
    public ClientHandler(Socket socket,Session session){
        this.session = session;
        this.socket = socket;
    }
    @Override
    public void run() {
        try(BufferedReader bufferedReader= new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(),true);) {
            String clientInput;
            while((clientInput=bufferedReader.readLine())!=null){
                String output = Router.route(clientInput,session);
                writer.println(output);
            }
            if(session.isInTransaction()){
                TransactionManager.getInstance().rollback(session.getTransactionId());
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
