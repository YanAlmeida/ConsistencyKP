package br.com.ufabc.redeBuscaArquivos;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mensagem {
    /* Classe para representar a mensagem trocada entre os peers */
    public String messageType;
    public String sender;
    public String solicitanteInicial;
    public String fileName;
    public String requestUUID;

    public Mensagem(String type, String origem, String solicitante, String file, String uuid) {
        /* Construtor para gerar mensagem a partir dos dados
         * (tipo, sender, solicitante inicial, nome do arquivo e uuid)
        */
        messageType = type;
        sender = origem;
        solicitanteInicial = solicitante;
        fileName = file;
        requestUUID = uuid;
    }

    public Mensagem(String jsonString) {
        /* Construtor para gerar mensagem a partir de sua representação como string em JSON */
        Pattern typePattern = Pattern.compile("\"messageType\": \"(.+?)\"");
        Pattern senderPattern = Pattern.compile("\"sender\": \"(.+?)\"");
        Pattern solicitanteInicialPattern = Pattern.compile("\"solicitanteInicial\": \"(.+?)\"");
        Pattern fileNamePattern = Pattern.compile("\"fileName\": \"(.+?)\"");
        Pattern requestUUIDPattern = Pattern.compile("\"requestUUID\": \"(.+?)\"");

        Matcher typeMatcher = typePattern.matcher(jsonString);
        Matcher senderMatcher = senderPattern.matcher(jsonString);
        Matcher solicitanteInicialMatcher = solicitanteInicialPattern.matcher(jsonString);
        Matcher fileNameMatcher = fileNamePattern.matcher(jsonString);
        Matcher requestUUIDMatcher = requestUUIDPattern.matcher(jsonString);

        typeMatcher.find();
        senderMatcher.find();
        solicitanteInicialMatcher.find();
        fileNameMatcher.find();
        requestUUIDMatcher.find();

        messageType = typeMatcher.group(1);
        sender = senderMatcher.group(1);
        solicitanteInicial = solicitanteInicialMatcher.group(1);
        fileName = fileNameMatcher.group(1);
        requestUUID = requestUUIDMatcher.group(1);
    }

    public String toJson() {
        /* Método para transformar a mensagem em string JSON */
        
        return String.format(
            "{\"messageType\": \"%s\", \"sender\": \"%s\", \"solicitanteInicial\": \"%s\", \"fileName\": \"%s\", \"requestUUID\": \"%s\"}",
            messageType,
            sender,
            solicitanteInicial,
            fileName,
            requestUUID
        );
    }
}