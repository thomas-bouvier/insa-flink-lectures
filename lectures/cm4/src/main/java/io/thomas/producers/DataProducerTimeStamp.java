package io.thomas.producers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataProducerTimeStamp
{
    public static void main(String[] args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);
        try{
                Socket socket = listener.accept();
                System.out.println("Got new connection: " + socket.toString());
                BufferedReader br = new BufferedReader(new FileReader("thermometers.txt"));
                try {
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    String measurement;
                    while ((measurement = br.readLine()) != null){
                        measurement += "  "+System.currentTimeMillis();                        
                        System.out.println(measurement);
                        out.println(measurement);
                           Thread.sleep(1000);
                    }
                } finally{
                    socket.close();
                }
        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
        }
    }
}


