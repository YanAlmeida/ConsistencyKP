package br.com.ufabc.consistencyKV;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Servidor {

    private ServerSocket socket;
    private InetAddress address;
    private Integer port;
    private String ipPortaLider;
    private List<String> servidores;
    private Boolean souLider;
    private ConcurrentHashMap<String, Integer> replicationOkCounter = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, Integer> timestamps = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, String> hashTable = new ConcurrentHashMap<String, String>();
    private Scanner inputUserScanner;
    private volatile Boolean initialized = false;

    public Servidor(Scanner scanner){
        inputUserScanner = scanner;
    }

    public ServerSocket getServerSocket() {
        return socket;
    }

    private class ThreadEnvio extends Thread {
        
        private Mensagem mensagemEnviar;

        public ThreadEnvio(Mensagem mensagem){
            mensagemEnviar = mensagem;
        }

        private void processaReplicationOk(Mensagem mensagem){

        }

        public void enviaMensagem(Mensagem mensagem) throws UnknownHostException, IOException{

            Socket socketClient = new Socket(getIpv4FromIpPort(mensagem.receiver), getPortFromIpPort(mensagem.receiver));
            
            OutputStream os = socketClient.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
    
            writer.writeBytes(mensagem.toJson());
    
            if(mensagem.messageType == "REPLICATION"){
                InputStreamReader is = new InputStreamReader(socketClient.getInputStream());
                BufferedReader reader = new BufferedReader(is);
    
                String messageJson = reader.readLine();
                socketClient.close();
    
                Mensagem mensagemRecebida = new Mensagem(messageJson);
                processaReplicationOk(mensagemRecebida);
                return;
            }

            socketClient.close();
    
        }

        public void run() {
            try {
                enviaMensagem(mensagemEnviar);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ThreadAtendimento extends Thread {

        private Socket socketNo;

        public ThreadAtendimento(Socket no) {
            socketNo = no;
        }

        private void trataGet(Mensagem mensagem){
            // AVALIAR O QUE FAZER CASO CHAVE NAO EXISTA

        }

        private void trataPut(Mensagem mensagem) throws UnknownHostException, IOException{
            if(souLider){
                String[] keyValueArray = parseKeyValueString(mensagem.data);
                hashTable.put(keyValueArray[0], keyValueArray[1]);

                if(!timestamps.containsKey(keyValueArray[0])){
                    timestamps.put(keyValueArray[0], 1);
                }else{
                    Integer oldTimestamp = timestamps.get(keyValueArray[0]);
                    timestamps.put(keyValueArray[0], oldTimestamp+1);
                }

                String ipPortaFormatado = getIpAddress(socket.getInetAddress().getAddress());
                String keyValueFormatado = String.format("%s:%s", keyValueArray[0], keyValueArray[1]);
                Integer timestamp = timestamps.get(keyValueArray[0]);
                String requestUUID = UUID.randomUUID().toString() + ":" + mensagem.sender;
                replicationOkCounter.put(requestUUID, 0);
                servidores.forEach(servidor -> {
                    Mensagem replicationMessage = new Mensagem("REPLICATION", ipPortaFormatado, servidor, timestamp, keyValueFormatado, requestUUID);
                    ThreadEnvio threadEnvio = new ThreadEnvio(replicationMessage);
                    threadEnvio.start();
                });

            }else{
                mensagem.receiver = ipPortaLider;
                ThreadEnvio threadEnvio = new ThreadEnvio(mensagem);
                threadEnvio.start();
            }
        }

        private void trataReplication(Mensagem mensagem) throws IOException{
            String[] keyValueArray = parseKeyValueString(mensagem.data);
            hashTable.put(keyValueArray[0], keyValueArray[1]);
            timestamps.put(keyValueArray[0], mensagem.timestamp);

            String ipPortaFormatado = getIpAddress(socket.getInetAddress().getAddress());
            
            OutputStream os = socketNo.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
            
            
            Mensagem mensagemReplicationOk = new Mensagem("REPLICATION_OK", ipPortaFormatado, ipPortaLider, null, null, mensagem.requestUUID);

            writer.writeBytes(mensagemReplicationOk.toJson());
        }


        public void run() {
            try{
                InputStreamReader is = new InputStreamReader(socketNo.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                String messageJson = reader.readLine();
                Mensagem mensagem = new Mensagem(messageJson);

                switch(mensagem.messageType){
                    case("GET"):
                        trataGet(mensagem);
                    break;

                    case("PUT"):
                        trataPut(mensagem);
                    break;

                    case("REPLICATION"):
                        trataReplication(mensagem);
                    break;

                }

            }catch(IOException e){
                e.printStackTrace();
            }finally{
                try{
                    socket.close();
                }catch(IOException e2){
                    e2.printStackTrace();
                }
            }
        }

    }

    private class ThreadMenu extends Thread {

        public void inicializaServidor(String lider, String ipAddress, Integer porta, List<String> servers) throws IOException{
            address = InetAddress.getByName(ipAddress);
            port = porta;
            ipPortaLider = lider;
            servidores = servers;
            if(ipPortaLider == getIpAddress(address.getAddress())){
                souLider = true;
            }else{
                souLider = false;
            }
            socket = new ServerSocket(port, 5, address);
            initialized = true;
        }

        public void run() {
            /* Execução da ThreadMenu, executa um loop para exibir as opções e colher a escolha do usuário,
             * executando o método correspondente
             */
            Boolean loop = true;
            while(loop) {
                System.out.println("Selecione uma das opções: \n1: INITIALIZE");
                String optionSelected = inputUserScanner.next();

                switch(optionSelected) {
                    case "1":
                        try {
                            List<String> servers = new ArrayList<String>();

                            System.out.println("Insira o IP: ");
                            String ipAddress = inputUserScanner.next();

                            System.out.println("Insira a porta: ");
                            Integer porta = Integer.parseInt(inputUserScanner.next());

                            System.out.println("Insira o IP:PORTA do lider: ");
                            String lider = inputUserScanner.next();

                            System.out.println("Insira o IP:PORTA do primeiro servidor: ");
                            servers.add(inputUserScanner.next());


                            System.out.println("Insira o IP:PORTA do segundo servidor: ");
                            servers.add(inputUserScanner.next());

                            inicializaServidor(lider, ipAddress, porta, servers);
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch(IOException e2){
                            e2.printStackTrace();
                        }
                    break;
                }
            }

        }
    }

    private static String[] parseKeyValueString(String keyValueString){
        Pattern keyValuePattern = Pattern.compile("(.+):(.+)");
        Matcher keyValueMatcher = keyValuePattern.matcher(keyValueString);

        keyValueMatcher.find();

        String[] retorno = new String[2];
        retorno[0] = keyValueMatcher.group(1);
        retorno[1] = keyValueMatcher.group(2);

        return retorno;
    }

    private static String getIpv4FromIpPort(String ipPortAddress){
        /* Método para extração do endereço de IP de string com formato IP:PORTA */
        Pattern ipPattern = Pattern.compile("(.+):\\d+");
        Matcher ipMatcher = ipPattern.matcher(ipPortAddress);

        ipMatcher.find();

        return ipMatcher.group(1);
    }

    private static Integer getPortFromIpPort(String ipPortAddress){
        /* Método para extração da porta de string com formato IP:PORTA */
        Pattern ipPattern = Pattern.compile(".+:(\\d+)");
        Matcher ipMatcher = ipPattern.matcher(ipPortAddress);

        ipMatcher.find();

        return Integer.parseInt(ipMatcher.group(1));
    }

    private static String getIpAddress(byte[] rawBytes) {
        /* Método para conversão de array de bytes em endereço IPv4 */
        int i = 4;
        StringBuilder ipAddress = new StringBuilder();
        for (byte raw : rawBytes) {
            ipAddress.append(raw & 0xFF);
            if (--i > 0) {
                ipAddress.append(".");
            }
        }
        return ipAddress.toString();
    }

    public static void main(String[] args) throws IOException{
        Scanner scanner = new Scanner(System.in);
        Servidor servidor = new Servidor(scanner);

        ThreadMenu threadMenu = servidor.new ThreadMenu();
        threadMenu.start();

        while(true){
            if(servidor.initialized){
                Socket socketRecebimento = servidor.getServerSocket().accept();
                ThreadAtendimento thread = servidor.new ThreadAtendimento(socketRecebimento);
                thread.start();
            }
            
        }

    }

}