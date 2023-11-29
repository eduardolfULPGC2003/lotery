import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class Server {
    public static final int PORT = 6789;
    private int pool = 0;
    private String path;

    public Server(String path, int pool) throws CsvValidationException {
        this.pool = pool;
        this.path = path;
        this.start();
    }

    private void start() throws CsvValidationException {
        DatagramSocket aSocket = null;
        try {
            aSocket = new DatagramSocket(PORT);
            byte[] buffer = new byte[1024];

            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (LocalDateTime.now().getMinute()==11) {
                        System.out.println("Ahora");
                        String now = LocalDateTime.now().withMinute(30).toString();
                        try {
                            System.out.println(lottery(now));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } catch (CsvValidationException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }, 0, 60000);

            while (true) {
                DatagramPacket req = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(req);
                String request = new String(req.getData(), 0, req.getLength());
                String response = null;
                if (request.equals("historic")){
                    response = getHistoric();
                } else {
                    response = accept(request);
                }
                buffer = response.getBytes();
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length,
                        req.getAddress(), req.getPort());
                aSocket.send(reply);

            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        } finally {
        }
    }

    private String getHistoric() {
        List<String> winners = new ArrayList<>();
        String path_win = this.path+"/winners.csv";
        try {
            CSVReader reader = new CSVReader(new FileReader(path_win));
            String[] wins;
            while ((wins = reader.readNext()) != null) {
                for (String win : wins) {
                    if (win.length() > 1) winners.add(win);
                }
            }
            return winners.toString();
        } catch (FileNotFoundException ie){
            return "No participants";
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String accept(String req) throws IOException, CsvValidationException {
        String[] parts = req.split("\\|");
        String fileName = parts[1].replace(":", "-") + ".csv";
        File file = new File(this.path, fileName);
        if (file.createNewFile()){
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(parts[0] + "|" + parts[2] + ",");
            writer.close();
        }
        else {
            String path = this.path+"/"+parts[1].replace(":","-")+".csv";
            CSVReader reader = new CSVReader(new FileReader(path));
            String[] shares;
            while ((shares=reader.readNext()) != null) {
                for (String share : shares) {
                    System.out.println(share);
                    String[] parts_share = share.split("\\|");
                    if (parts_share[0].equals(parts[0]) && parts_share[1].equals(parts[2]))
                        return "You can't play twice with the same number";
                }
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(parts[0] + "|" + parts[2] + ",");
            writer.close();
        }
        pool += 100;
        return "Thank you for participating: " + req;
    }

    private String lottery(String now) throws IOException, CsvValidationException {
        List<String> winners = new ArrayList<>();
        Integer winner_num = new Random().nextInt(256);
        String path_participants = this.path+"/" + now.replace(":","-").substring(0,16) + ".csv";
        try {
            CSVReader reader = new CSVReader(new FileReader(path_participants));
            String[] shares;
            while ((shares = reader.readNext()) != null) {
                for (String share : shares) {
                    System.out.println(share);
                    String[] parts_share = share.split("\\|");
                    if (parts_share.length == 2) {
                        if (Integer.parseInt(parts_share[1]) == winner_num)
                            winners.add(parts_share[0]);
                    }
                }
            }
            reader.close();
            new File(path_participants).delete();
            System.out.println(path_participants);
            if (winners.size() == 0) return "Number: " + winner_num + ". No winners";
            else {
                int ppw = pool / winners.size();
                File file = new File(this.path + "/winners.csv");
                BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
                for (String winner : winners) {
                    writer.write(now.substring(0, 16) + "|" + winner + "|" + ppw + ",");
                }
                writer.close();
                pool = 0;
                return "The winner number is " + winner_num + ". The winners are" + winners.toString() + ". Each one won:" + ppw;
            }
        } catch (FileNotFoundException ie){
            return "No participants";
        }
    }

    public static void main(String args[]) throws CsvValidationException {
        new Server("files", 200);
    }
}
