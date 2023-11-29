import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Scanner;

public class Client {
    private String email;

    public Client(String email) {
        this.email = email;
    }

    private String play() throws ParseException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Lets play!");
        LocalDateTime dateTime = getDate();
        System.out.println("Please introduce a number between 0-255: ");
        int number = scanner.nextInt();
         if (dateTime.isBefore(LocalDateTime.now())) {
             return "Wrong Date";
        } else if (number > 255 | number < 0) {
             return "Wrong Number. Must be between 0 and 255";
        } else {
             String message = this.email + "|" + dateTime.toString() + "|" + number;
             String reply = request(message);
             return reply;
        }
    }

    private LocalDateTime getDate(){
        Scanner scanner = new Scanner(System.in);
        System.out.println("When do you want to play? (Introduce de number in the format DD/MM/YY)");
        String date = scanner.nextLine();
        try{
            LocalDate parsedDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("dd/MM/yy"));
            System.out.println("Now introduce de time in the format: HH:MM (Note that we play every hour at :30)");
            String time = scanner.nextLine();
            LocalTime parsedTime = LocalTime.parse(time, DateTimeFormatter.ofPattern("HH:mm")).withMinute(30);
            LocalDateTime dateTime = parsedDate.atTime(parsedTime);
            return dateTime;
        } catch (DateTimeParseException pe){
            System.out.println("Incorrect format: "+ pe);
            System.out.println("----------------------------------------------");
            System.exit(0);
        }
        return null;
    }

    private String request(String message){
        DatagramSocket aSocket = null;
        try {
            aSocket = new DatagramSocket();
            byte[] buffer = message.getBytes();
            DatagramPacket req = new DatagramPacket(buffer, buffer.length,
                    InetAddress.getByName("localhost"), Server.PORT);
            aSocket.send(req);
            buffer = new byte[1024];
            DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
            aSocket.receive(reply);
            String receivedMessage = new String(reply.getData(), 0, reply.getLength());
            return receivedMessage;
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        } finally {
            if(aSocket != null) aSocket.close();
        }
        return null;
    }

    private String retrieve_historic() {
        String reply = request("historic");
        return reply;
    }

    public static void main(String args[]) throws ParseException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Welcome to Lottery365. Bet whenever you want");
        System.out.print("Please enter your email: ");
        String regex = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$";
        String email = scanner.nextLine();
        if (!email.matches(regex)){
            System.out.println("Wrong email. Try again.");
            System.exit(0);
        }
        Client client = new Client(email);
        mainLoop:
        while (true){
            System.out.println("What do you want to do?\n1. Play\n2. Get winners\n3. Exit");
            System.out.print("Your selection: ");
            int number = scanner.nextInt();
            switch (number){
                case 1:
                    System.out.println(client.play());
                    break;
                case 2:
                    System.out.println(client.retrieve_historic());
                    break;
                case 3:
                    System.out.println("Good bye!");
                    break mainLoop;
            }
        }
    }
}

