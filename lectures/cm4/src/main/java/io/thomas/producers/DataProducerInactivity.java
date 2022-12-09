package io.thomas.producers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataProducerInactivity
{
    public static void main(String[] args) throws IOException
    {
        ServerSocket listener = new ServerSocket(9090);
        try{
                Socket socket = listener.accept();
                System.out.println("Got new connection: " + socket.toString());
                BufferedReader br = new BufferedReader(new FileReader("thermometers.txt"));
                int eventsSent = 0;
                try {
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    String measurement;
                    while ((measurement = br.readLine()) != null){
                    	eventsSent++;
                    	System.out.println(measurement);
                        out.println(measurement);
                        
                        if (eventsSent>=4) {
                        	eventsSent = 0;
                        	System.out.println("--------------------");
                        	Thread.sleep(3000);                        	
                        } else
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
