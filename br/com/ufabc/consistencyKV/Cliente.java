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
         * mensagens aos servidores (GET ou PUT), e viabiliza o processamento do retorno do GET (mensagens RESPONSE)
         */  
        Mensagem mensagemEnvio;

        public ThreadEnvio(Mensagem enviar){
            /* Construtor que recebe a mensagem a ser enviada */
            mensagemEnvio = enviar;
        }

        public void processaRespostaGet(Mensagem mensagemRecebida){
            /* 
             * Método que processa mensagens RESPONSE e erros (enviadas em resposta ao GET),
             * atualizando o timestamp no caso do RESPONSE e exibindo os valores ou exibindo mensagens de erro
             * personalizadas para os erros TRY_OTHER_SERVICE_OR_LATER e NOT_FOUND
             */
            String[] keyValueArray = parseKeyValueString(mensagemRecebida.data);
            switch(mensagemRecebida.messageType){
                case "RESPONSE":
                    timestamps.put(keyValueArray[0], mensagemRecebida.timestamp);
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
                    System.out.println(String.format(
                        "Erro de consistência encontrado. Tente novamente mais tarde. Meu timestamp %s e timestamp do servidor (%s) %s",
                        timestamps.getOrDefault(keyValueArray[0], 0),
                        mensagemRecebida.sender,
                        mensagemRecebida.timestamp
                    ));
                break;

                case "NOT_FOUND":
                    System.out.println(String.format(
                        "Erro retornado. A key informada não foi encontrada. Meu timestamp %s e timestamp do servidor (%s) %s",
                        timestamps.getOrDefault(keyValueArray[0], 0),
                        mensagemRecebida.sender,
                        mensagemRecebida.timestamp
                    ));
                break;

            }
        }

        public void enviaMensagem(Mensagem mensagem) throws UnknownHostException, IOException{
            /* 
             * Método para o envio de mensagens a outros servidores ou clientes, 
             * utilizando a classe Mensagem e seus atributos para efetuar a conexão com eles via TCP.
             * Caso seja uma mensagem do tipo GET, aguarda a mensagem de resposta (RESPONSE ou erros) 
             * para processamento
             */
            Socket socketC = new Socket(getIpv4FromIpPort(mensagem.receiver), getPortFromIpPort(mensagem.receiver));
            
            try{
                OutputStream os = socketC.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);
        
                writer.writeBytes(mensagem.toJson() + "\n");
        
                if(mensagem.messageType.equals("GET")){
                    InputStreamReader is = new InputStreamReader(socketC.getInputStream());
                    BufferedReader reader = new BufferedReader(is);
        
                    String messageJson = reader.readLine();
        
                    Mensagem mensagemRecebida = new Mensagem(messageJson);
                    processaRespostaGet(mensagemRecebida);
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
            try{
                enviaMensagem(mensagemEnvio);
            }catch(IOException e){
                e.printStackTrace();
                return;
            }
            
        }
    }

    private class ThreadResposta extends Thread {
        /* Classe aninhada que representa a thread de atendimento a
           conexões recebidas do servidor líder (desconhecido) */
        private Socket socketNo;

        public ThreadResposta(Socket no) {
            /* 
             * Construtor que recebe o socket gerado após o accept() para comunicação 
             * com o servidor líder (desconhecido)
             */
            socketNo = no;
        }

        private void trataPutOK(Mensagem mensagem){
            /*
             * Método para tratamento da mensagem do tipo PUT_OK, exibindo print na tela e atualizando o timestamp
             * da entrada correspondente
             */
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
            /* 
             * Execução da ThreadResposta, efetua a leitura da mensagem enviada pelo servidor líder (desconhecido)
             * e direciona ela para o tratamento (PUT_OK)
             */
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
        public void inicializaCliente(List<String> servidores) throws IOException{
            /* 
             * Método correspondente à opção "INITIALIZE" do menu. Responsável por colher
             * o endereço/porta dos servidores, além de criar o ServerSocket,
             * responsável por ouvir em alguma porta disponível do cliente.
             */
            if(initialized){
                initialized = false;
                socket.close();
            }
            socket = new ServerSocket(0);
            address = socket.getInetAddress();
            port = socket.getLocalPort();
            servidorList = servidores;
            initialized = true;
        }

        public void enviaPut(String key, String value){
            /* 
             * Método correspondente à opção "PUT" do menu, permite que o cliente gere uma mensagem do tipo
             * PUT, contendo uma chave e um valor, e envie para um servidor aleatório através da ThreadEnvio
             */
            String origemFormatada = String.format("%s:%s", getIpAddress(address.getAddress()), port);
            String randomServer = servidorList.get(new Random().nextInt(servidorList.size()));
            String keyValueFormatado = String.format("%s:%s", key, value);
            Integer timestampEnvio = timestamps.getOrDefault(key, 0) + 1;
            timestamps.put(key, timestampEnvio);
            Mensagem mensagemEnviar = new Mensagem("PUT", origemFormatada, randomServer, timestampEnvio, keyValueFormatado);
    
            Thread envioPut = new ThreadEnvio(mensagemEnviar);
            envioPut.start();
        }
    
        public void enviaGet(String key){
            /* 
             * Método correspondente à opção "GET" do menu, permite que o cliente gere uma mensagem do tipo
             * GET, contendo uma chave, e envie para um servidor aleatório através da ThreadEnvio
             */
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
                        if(!initialized){
                            System.out.println("Inicialize o cliente primeiro.");
                            continue;
                        }
                        System.out.println("Insira a chave para o GET: ");
                        String keyGet = inputUserScanner.next();
                        enviaGet(keyGet);
                    break;

                    case "3":
                        if(!initialized){
                            System.out.println("Inicialize o cliente primeiro.");
                            continue;
                        }
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
         * Método main, responsável por instânciar a classe Cliente, startar a ThreadMenu e
         * ouvir requisições (por ex., o PUT_OK do líder, que é desconhecido)
         * através de um loop e do ServerSocket.accept(), que culminam no start da ThreadResposta
         */
        Scanner scanner = new Scanner(System.in);
        Cliente cliente = new Cliente(scanner);

        ThreadMenu threadMenu = cliente.new ThreadMenu();
        threadMenu.start();

        while(true){
            if(cliente.initialized){
                try{
                    Socket socketRecebimento = cliente.getServerSocket().accept();
                    ThreadResposta thread = cliente.new ThreadResposta(socketRecebimento);
                    thread.start();
                }catch(SocketException e){
                    e.printStackTrace();
                }

            }
            
        }

    }

}