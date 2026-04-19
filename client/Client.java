package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client {

    public static void main(String[]args){
        try (
                Socket socket = new Socket("localhost", 8080);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                Scanner scanner = new Scanner(System.in)
        ) {
            while (true) {
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("exit")) {
                    break;  // exits loop, try-with-resources closes everything
                }
                writer.println(line);
                String response;
                while ((response = reader.readLine()) != null && !response.equals("END")) {
                    System.out.println(response);
                }
            }
        } catch (IOException e) {
            System.out.println("Could not connect to server: " + e.getMessage());
        }

    }
}
