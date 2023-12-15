package io.thomas.producers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataProducer
{
    public static void main(String[] args) throws IOException
    {
        try (ServerSocket listener = new ServerSocket(9090)) {
            Socket socket = listener.accept();
            try (socket) {
                System.out.println("Got new connection: " + socket.toString());
                BufferedReader br = new BufferedReader(new FileReader("thermometers.txt"));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String measurement;
                while ((measurement = br.readLine()) != null) {
                    System.out.println(measurement);
                    out.println(measurement);
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
