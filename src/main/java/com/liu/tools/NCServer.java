package com.liu.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 * 参照 https://blog.csdn.net/dwb502/article/details/103219238
 */
public class NCServer {

    private static final int PORT = 17777;

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket server = new ServerSocket(PORT);
        Socket socket = server.accept();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("[" + simpleDateFormat.format(new Date()) + "]"
                + socket.getInetAddress() + "已建立连接");

        // 监控连接是否断开
//        new Thread(new CheckClientThread(socket)).start();

        // 输出流
        OutputStream outputStream = socket.getOutputStream();

        // 在控制台输入
        Scanner in = new Scanner(System.in);

        while (true) {
            String str = in.nextLine() + "\n";
            outputStream.write(str.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();

//            String str = "sensor_1,1547718207,36.3" + "\n";
//            outputStream.write(str.getBytes(StandardCharsets.UTF_8));
//            outputStream.flush();

//            Thread.sleep(300);
        }

    }

    /**
     * 监控连接程序是否断开的线程类
     */
    public static class CheckClientThread implements Runnable {

        private Socket socketClient;

        public CheckClientThread(Socket socketClient) {
            this.socketClient = socketClient;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    socketClient.sendUrgentData(0xFF);
                } catch (IOException e) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    System.out.println("[" + simpleDateFormat.format(new Date()) + "]"
                            + socketClient.getInetAddress() + "连接已关闭");
                    System.exit(0);
                }
            }
        }
    }
}
