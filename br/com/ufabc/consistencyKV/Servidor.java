package br.com.ufabc.consistencyKV;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.TimeUnit;
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
        /* 
         * Construtor recebendo scanner para uso em diversos métodos/threads.
         */
        inputUserScanner = scanner;
    }

    public ServerSocket getServerSocket() {
        /* 
         * Método getter para retorno do ServerSocket
         */
        return socket;
    }

    private class ThreadEnvio extends Thread {
        /* 
         * Classe aninhada que representa a Thread que efetua o envio de 
         * mensagens aos clientes (PUT_OK) ou a outros servidores (REPLICATION)
         */  
        private Mensagem mensagemEnviar;

        public ThreadEnvio(Mensagem mensagem){
            /* Construtor que recebe a mensagem a ser enviada */
            mensagemEnviar = mensagem;
        }

        private void processaReplicationOk(Mensagem mensagem) throws UnknownHostException, IOException{
            /* 
             * Método que processa mensagens REPLICATION_OK (enviadas em resposta ao REPLICATION)
             * e, caso todos os servidores tenham enviado, efetua o envio da mensagem PUT_OK para os clientes 
             */
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
            /* 
             * Método para o envio de mensagens a outros servidores ou clientes, 
             * utilizando a classe Mensagem e seus atributos para efetuar a conexão com eles via TCP.
             * Caso seja uma mensagem do tipo REPLICATION, aguarda a mensagem de resposta (REPLICATION_OK) 
             * para processamento
             */
            Socket socketC = new Socket(getIpv4FromIpPort(mensagem.receiver), getPortFromIpPort(mensagem.receiver));
            
            try{
                OutputStream os = socketC.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);
        
                writer.writeBytes(mensagem.toJson() + "\n");
        
                if(mensagem.messageType.equals("REPLICATION")){
                    InputStreamReader is = new InputStreamReader(socketC.getInputStream());
                    BufferedReader reader = new BufferedReader(is);
        
                    String messageJson = reader.readLine();
        
                    Mensagem mensagemRecebida = new Mensagem(messageJson);
                    processaReplicationOk(mensagemRecebida);
                    return;
                }
            }finally{
                socketC.close();
            }

    
        }

        public void run() {
            /* 
             * Execução da ThreadEnvio, executa o método enviaMensagem, 
             * passando a mensagem recebida no construtor 
             */
            try {
                enviaMensagem(mensagemEnviar);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ThreadAtendimento extends Thread {
        /* Classe aninhada que representa a thread de atendimento a
           conexões recebidas dos clientes/outros servidores */
        private Socket socketNo;

        public ThreadAtendimento(Socket no) {
            /* 
             * Construtor que recebe o socket gerado após o accept() para comunicação 
             * com o servidor ou cliente conectado
             */
            socketNo = no;
        }

        private void trataGet(Mensagem mensagem) throws IOException{
            /* 
             * Método para tratamento de mensagem do tipo GET recebida. Efetua a busca da key na hashTable, 
             * avalia os timestamps e, de acordo com o caso, retorna a chave e o valor, o erro TRY_OTHER_SERVICE_OR_LATER
             * ou o erro NOT_FOUND.
             */
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
                String.format("%s:%s", key, value)
            );

            String devolvendo;

            if(timestamp < clientTimestamp){
                mensagemResposta.messageType = "TRY_OTHER_SERVICE_OR_LATER";
                devolvendo = "erro";
            }else if(value == null && timestamp.equals(0)){
                mensagemResposta.messageType = "NOT_FOUND";
                devolvendo = "mensagem NOT_FOUND (valor é null)";
            }else{
                mensagemResposta.messageType = "RESPONSE";
                devolvendo = value;
            }

            System.out.println(String.format("Cliente %s GET key: %s ts: %s. Meu ts é %s, portanto devolvendo %s", clienteIpPortaFormatado, key, clientTimestamp, timestamp, devolvendo));
            writer.writeBytes(mensagemResposta.toJson() + "\n");
        }

        private void trataPut(Mensagem mensagem) throws UnknownHostException, IOException{
            /*
             * Método para o tratamento de mensagem do tipo PUT recebida. Caso não seja o líder, 
             * encaminha a mensagem para o líder. Caso seja o líder, efetua o PUT na hashTable, 
             * atualiza o timestamp e envia uma mensagem REPLICATION
             * para cada servidor através da thread de envio, que processará o retorno dos 
             * REPLICATION_OK e tratará do envio do PUT_OK
             */
            // try{
            //     TimeUnit.SECONDS.sleep(5); // Para testes relacionados ao erro TRY_OTHER_SERVICE_OR_LATER
            // }catch(InterruptedException e){
            //     e.printStackTrace();
            // }
            
            String[] keyValueArray = parseKeyValueString(mensagem.data);
            if(souLider){
                hashTable.put(keyValueArray[0], keyValueArray[1]);

                String clienteIpPortaFormatado = getIpAddress(socketNo.getInetAddress().getAddress()) + ":" + socketNo.getPort();

                System.out.println(String.format("Cliente %s PUT key:%s value:%s", clienteIpPortaFormatado, keyValueArray[0], keyValueArray[1]));

                Integer possibleTimestamp = timestamps.getOrDefault(keyValueArray[0], 0) + 1;
                timestamps.put(keyValueArray[0], Math.max(mensagem.timestamp, possibleTimestamp));

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
            /*
             * Método para o tratamento de mensagem do tipo REPLICATION recebida. Efetua a atualização da
             * entrada na hashtable e do timestamp dessa entrada, e após isso retorna uma mensagem do tipo 
             * REPLICATION_OK para o líder
             */
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
            /* 
             * Execução da ThreadAtendimento, efetua a leitura da mensagem enviada pelo cliente/outro servidor
             * e direciona ela para o tratamento correto de acordo com seu tipo
             */
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
        /* 
         * Classe aninhada que representa a thread que executa o menu interativo e possui os métodos
         * correspondentes a suas opções
         */

        public void inicializaServidor(String lider, String ipAddress, Integer porta, List<String> servers) throws IOException{
            /* 
             * Método correspondente à opção "INITIALIZE" do menu. Responsável por colher dados como:
             * O endereço/porta do servidor, endereço/porta do líder e endereço/porta dos outros servidores,
             * além de criar o ServerSocket, responsável por ouvir a porta especificada.
             */
            if(initialized){
                initialized = false;
                socket.close();
            }
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
        /*
         * Método estático para separar o requestUUID de mensagens do tipo replication,
         * que contém, além do UUID, o endereço e porta do cliente
         */
        Pattern replicationUUIDPattern = Pattern.compile("(.+)\\$(.+)");
        Matcher replicationUUIDMatcher = replicationUUIDPattern.matcher(replicationUUID);

        replicationUUIDMatcher.find();

        String[] retorno = new String[2];
        retorno[0] = replicationUUIDMatcher.group(1);
        retorno[1] = replicationUUIDMatcher.group(2);

        return retorno;
    }

    private static String[] parseKeyValueString(String keyValueString){
        /*
         * Método estático para separar a chave e o valor do campo data das mensagens
         */
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
        /* 
         * Método main, responsável por instânciar a classe Servidor, startar a ThreadMenu e
         * ouvir novas requisições através de um loop e do ServerSocket.accept(), 
         * que culminam no start da ThreadAtendimento
         */
        Scanner scanner = new Scanner(System.in);
        Servidor servidor = new Servidor(scanner);

        ThreadMenu threadMenu = servidor.new ThreadMenu();
        threadMenu.start();

        while(true){
            if(servidor.initialized){
                try{
                    Socket socketRecebimento = servidor.getServerSocket().accept();
                    ThreadAtendimento thread = servidor.new ThreadAtendimento(socketRecebimento);
                    thread.start();
                }catch(SocketException e){
                    e.printStackTrace();
                }

            }
            
        }

    }

}