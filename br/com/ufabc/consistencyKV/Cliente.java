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
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Cliente {

    private ServerSocket socket;
    private InetAddress address;
    private Integer port;
    private List<String> servidorList;
    private ConcurrentHashMap<String, Integer> timestamps = new ConcurrentHashMap<String, Integer>();
    private Scanner inputUserScanner;
    private volatile Boolean initialized = false;

    public Cliente(Scanner scanner){
        inputUserScanner = scanner;
    }

    public ServerSocket getServerSocket() {
        return socket;
    }

    private class ThreadEnvio extends Thread {
        Mensagem mensagemEnvio;

        public ThreadEnvio(Mensagem enviar){
            mensagemEnvio = enviar;
        }

        public void processaRespostaGet(Mensagem mensagemRecebida){
            switch(mensagemRecebida.messageType){
                case "RESPONSE":
                    String[] keyValueArray = parseKeyValueString(mensagemRecebida.data);
                    System.out.println(String.format(
                        "GET key: %s value: %s obtido do servidor %s, meu timestamp %s e do servidor %s",
                        keyValueArray[0],
                        keyValueArray[1],
                        mensagemRecebida.sender,
                        timestamps.get(keyValueArray[0]),
                        mensagemRecebida.timestamp
                        ));
                break;
    
                case "TRY_OTHER_SERVICE_OR_LATER":
                    System.out.println("Erro retornado. Tente novamente mais tarde.");
                break;

            }
        }

        public void enviaMensagem(Mensagem mensagem) throws UnknownHostException, IOException{

            Socket socketClient = new Socket(getIpv4FromIpPort(mensagem.receiver), getPortFromIpPort(mensagem.receiver));
            
            OutputStream os = socketClient.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
    
            writer.writeBytes(mensagem.toJson());
    
            if(mensagem.messageType == "GET"){
                InputStreamReader is = new InputStreamReader(socketClient.getInputStream());
                BufferedReader reader = new BufferedReader(is);
    
                String messageJson = reader.readLine();
                socketClient.close();
    
                Mensagem mensagemRecebida = new Mensagem(messageJson);
                processaRespostaGet(mensagemRecebida);
                return;
            }
    
            socketClient.close();
    
    
        }

        public void run() {
            try{
                enviaMensagem(mensagemEnvio);
            }catch(IOException e){
                e.printStackTrace();
                return;
            }
            
        }
    }

    private class ThreadResposta extends Thread {

        private Socket socketNo;

        public ThreadResposta(Socket no) {
            socketNo = no;
        }

        private void trataPutOK(Mensagem mensagem){
            String[] keyValueArray = parseKeyValueString(mensagem.data);
            timestamps.put(keyValueArray[0], mensagem.timestamp);
            System.out.println(String.format(
                "PUT_OK key: %s value: %s timestamp: %s realizada no servidor %s",
                keyValueArray[0],
                keyValueArray[1],
                mensagem.timestamp,
                mensagem.sender
                ));
            return;
        }

        public void run() {
            try{
                InputStreamReader is = new InputStreamReader(socketNo.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                String messageJson = reader.readLine();
                Mensagem mensagem = new Mensagem(messageJson);

                switch(mensagem.messageType){
                    case("PUT_OK"):
                        trataPutOK(mensagem);
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

        public void inicializaCliente(List<String> servidores) throws IOException{
            socket = new ServerSocket(0);
            address = socket.getInetAddress();
            port = socket.getLocalPort();
            servidorList = servidores;
            initialized = true;
        }

        public void enviaPut(String key, String value){
            String origemFormatada = String.format("%s:%s", getIpAddress(address.getAddress()), port);
            String randomServer = servidorList.get(new Random().nextInt(servidorList.size()));
            String keyValueFormatado = String.format("%s:%s", key, value);
            Mensagem mensagemEnviar = new Mensagem("PUT", origemFormatada, randomServer, null, keyValueFormatado);
    
            Thread envioPut = new ThreadEnvio(mensagemEnviar);
            envioPut.start();
        }
    
        public void enviaGet(String key){
            String origemFormatada = String.format("%s:%s", getIpAddress(address.getAddress()), port);
            String randomServer = servidorList.get(new Random().nextInt(servidorList.size()));
    
            if(!timestamps.containsKey(key)){
                timestamps.put(key, 0);
            }
    
            Mensagem mensagemEnviar = new Mensagem("GET", origemFormatada, randomServer, timestamps.get(key), key);
    
            Thread envioGet = new ThreadEnvio(mensagemEnviar);
            envioGet.start();
        }

        public void run() {
            /* Execução da ThreadMenu, executa um loop para exibir as opções e colher a escolha do usuário,
             * executando o método correspondente
             */
            Boolean loop = true;
            while(loop) {
                System.out.println("Selecione uma das opções: \n1: INITIALIZE\n2: GET\n3: PUT");
                String optionSelected = inputUserScanner.next();

                switch(optionSelected) {
                    case "1":
                        try {
                            List<String> listaServidores = new ArrayList<String>();
                            System.out.println("Insira o IP:PORTA do primeiro servidor");
                            listaServidores.add(inputUserScanner.next());
                            System.out.println("Insira o IP:PORTA do segundo servidor");
                            listaServidores.add(inputUserScanner.next());
                            System.out.println("Insira o IP:PORTA do terceiro servidor");
                            listaServidores.add(inputUserScanner.next());
                            inicializaCliente(listaServidores);
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch(IOException e2){
                            e2.printStackTrace();
                        }
                    break;

                    case "2":
                        System.out.println("Insira a chave para o GET: ");
                        String keyGet = inputUserScanner.next();
                        enviaGet(keyGet);
                    break;

                    case "3":
                        System.out.println("Insira a chave para o PUT: ");
                        String keyPut = inputUserScanner.next();
                        System.out.println("Insira o valor para o PUT: ");
                        String valuePut = inputUserScanner.next();
                        enviaPut(keyPut, valuePut);
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
        Cliente cliente = new Cliente(scanner);

        ThreadMenu threadMenu = cliente.new ThreadMenu();
        threadMenu.start();

        while(true){
            if(cliente.initialized){
                Socket socketRecebimento = cliente.getServerSocket().accept();
                ThreadResposta thread = cliente.new ThreadResposta(socketRecebimento);
                thread.start();
            }
            
        }

    }

}