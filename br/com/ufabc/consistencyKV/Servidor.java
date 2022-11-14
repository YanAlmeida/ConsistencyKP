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

        private void processaReplicationOk(Mensagem mensagem) throws UnknownHostException, IOException{
            replicationOkCounter.compute(mensagem.requestUUID, (k, v) -> v + 1);

            if(replicationOkCounter.get(mensagem.requestUUID) >= servidores.size()){
                String ipPortaFormatado = getIpAddress(socket.getInetAddress().getAddress()) + ":" + port;
                String[] uuidParseado = parseReplicationUUID(mensagem.requestUUID);
                String[] keyValueArray = parseKeyValueString(mensagem.data);
                Integer timestampEnviar = timestamps.get(keyValueArray[0]);
                System.out.println(String.format("Enviando PUT_OK ao Cliente %s da key:%s ts:%s", uuidParseado[1], keyValueArray[0], timestampEnviar));
                Mensagem putOk = new Mensagem("PUT_OK", ipPortaFormatado, uuidParseado[1], timestampEnviar, mensagem.data);
                enviaMensagem(putOk);
            }
        }

        public void enviaMensagem(Mensagem mensagem) throws UnknownHostException, IOException{

            Socket socketClient = new Socket(getIpv4FromIpPort(mensagem.receiver), getPortFromIpPort(mensagem.receiver));
            
            try{
                OutputStream os = socketClient.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);
        
                writer.writeBytes(mensagem.toJson() + "\n");
        
                if(mensagem.messageType.equals("REPLICATION")){
                    InputStreamReader is = new InputStreamReader(socketClient.getInputStream());
                    BufferedReader reader = new BufferedReader(is);
        
                    String messageJson = reader.readLine();
        
                    Mensagem mensagemRecebida = new Mensagem(messageJson);
                    processaReplicationOk(mensagemRecebida);
                    return;
                }
            }finally{
                socketClient.close();
            }

    
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

        private void trataGet(Mensagem mensagem) throws IOException{
            String key = mensagem.data;
            String value = hashTable.get(key);
            Integer timestamp = timestamps.getOrDefault(key, 0);
            Integer clientTimestamp = mensagem.timestamp;

            String ipPortaFormatado = getIpAddress(socket.getInetAddress().getAddress()) + ":" + port;
            String clienteIpPortaFormatado = getIpAddress(socketNo.getInetAddress().getAddress()) + ":" + socketNo.getPort();

            OutputStream os = socketNo.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            Mensagem mensagemResposta = new Mensagem(
                null,
                ipPortaFormatado,
                clienteIpPortaFormatado,
                timestamp,
                null
            );

            String devolvendo;

            if(timestamp < clientTimestamp){
                mensagemResposta.messageType = "TRY_OTHER_SERVICE_OR_LATER";
                devolvendo = "erro";
            }else if(value == null){
                mensagemResposta.messageType = "NOT_FOUND";
                devolvendo = "mensagem NOT_FOUND (valor é null)";
            }else{
                mensagemResposta.messageType = "RESPONSE";
                mensagemResposta.data = String.format("%s:%s", key, value);
                devolvendo = value;
            }

            System.out.println(String.format("Cliente %s GET key: %s ts: %s. Meu ts é %s, portanto devolvendo %s", clienteIpPortaFormatado, key, clientTimestamp, timestamp, devolvendo));
            writer.writeBytes(mensagemResposta.toJson() + "\n");
        }

        private void trataPut(Mensagem mensagem) throws UnknownHostException, IOException{
            String[] keyValueArray = parseKeyValueString(mensagem.data);
            if(souLider){
                hashTable.put(keyValueArray[0], keyValueArray[1]);

                String clienteIpPortaFormatado = getIpAddress(socketNo.getInetAddress().getAddress()) + ":" + socketNo.getPort();

                System.out.println(String.format("Cliente %s PUT key:%s value:%s", clienteIpPortaFormatado, keyValueArray[0], keyValueArray[1]));

                timestamps.put(keyValueArray[0], Math.max(mensagem.timestamp, timestamps.getOrDefault(keyValueArray[0], 0)));

                String ipPortaFormatado = getIpAddress(socket.getInetAddress().getAddress()) + ":" + port;
                String keyValueFormatado = String.format("%s:%s", keyValueArray[0], keyValueArray[1]);
                Integer timestamp = timestamps.get(keyValueArray[0]);
                String requestUUID = UUID.randomUUID().toString() + "$" + mensagem.sender;
                replicationOkCounter.put(requestUUID, 0);
                servidores.forEach(servidor -> {
                    Mensagem replicationMessage = new Mensagem("REPLICATION", ipPortaFormatado, servidor, timestamp, keyValueFormatado, requestUUID);
                    ThreadEnvio threadEnvio = new ThreadEnvio(replicationMessage);
                    threadEnvio.start();
                });

            }else{
                System.out.println(String.format("Encaminhando PUT key:%s value:%s", keyValueArray[0], keyValueArray[1]));
                mensagem.receiver = ipPortaLider;
                ThreadEnvio threadEnvio = new ThreadEnvio(mensagem);
                threadEnvio.start();
            }
        }

        private void trataReplication(Mensagem mensagem) throws IOException{
            String[] keyValueArray = parseKeyValueString(mensagem.data);
            System.out.println(String.format("REPLICATION key:%s value:%s ts:%s", keyValueArray[0], keyValueArray[1], mensagem.timestamp));
            hashTable.put(keyValueArray[0], keyValueArray[1]);
            timestamps.put(keyValueArray[0], mensagem.timestamp);

            String ipPortaFormatado = getIpAddress(socket.getInetAddress().getAddress()) + ":" + port;
            
            OutputStream os = socketNo.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
            
            Mensagem mensagemReplicationOk = new Mensagem("REPLICATION_OK", ipPortaFormatado, ipPortaLider, null, mensagem.data, mensagem.requestUUID);

            writer.writeBytes(mensagemReplicationOk.toJson() + "\n");
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
                    socketNo.close();
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
            if(ipPortaLider.equals(getIpAddress(address.getAddress()) + ":" + port)){
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

    private static String[] parseReplicationUUID(String replicationUUID){
        Pattern replicationUUIDPattern = Pattern.compile("(.+)\\$(.+)");
        Matcher replicationUUIDMatcher = replicationUUIDPattern.matcher(replicationUUID);

        replicationUUIDMatcher.find();

        String[] retorno = new String[2];
        retorno[0] = replicationUUIDMatcher.group(1);
        retorno[1] = replicationUUIDMatcher.group(2);

        return retorno;
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