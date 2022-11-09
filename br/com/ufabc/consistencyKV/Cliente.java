package br.com.ufabc.consistencyKV;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Cliente {

    private Socket socket;
    private InetAddress address;
    private Integer port;
    private List<String> servidorList;
    private ConcurrentHashMap<String, Integer> timestamps;

    public void enviaMensagem(Mensagem mensagem){
        
    }

    public void inicializaCliente(List<String> servidores, InetAddress clientAddress, Integer clientPort) throws IOException{
        address = clientAddress;
        port = clientPort;
        socket = new Socket(clientAddress, clientPort);
        servidorList = servidores;
    }

    public void enviaPut(String key, String value){

    }

    public void enviaGet(String key){

    }

    private class ThreadRespostas extends Thread {

    }

    public static void main(String[] args){

    }

}