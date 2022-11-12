package br.com.ufabc.consistencyKV;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mensagem {
    /* Classe para representar a mensagem trocada entre os peers */
    public String messageType;
    public String sender;
    public String receiver;
    public Integer timestamp;
    public String data;

    public Mensagem(String type, String origem, String destino, Integer timestampEnvio, String dados) {
        /* Construtor para gerar mensagem a partir dos dados
         * (tipo, sender, solicitante inicial, nome do arquivo e uuid)
        */
        messageType = type;
        sender = origem;
        receiver = destino;
        timestamp = timestampEnvio;
        data = dados;
    }

    public Mensagem(String jsonString) {
        /* Construtor para gerar mensagem a partir de sua representação como string em JSON */
        Pattern typePattern = Pattern.compile("\"messageType\": \"(.+?)\"");
        Pattern senderPattern = Pattern.compile("\"sender\": \"(.+?)\"");
        Pattern receiverPattern = Pattern.compile("\"receiver\": \"(.+?)\"");
        Pattern timestampPattern = Pattern.compile("\"timestamp\": \"(.+?)\"");
        Pattern dataPattern = Pattern.compile("\"data\": \"(.+?)\"");

        Matcher typeMatcher = typePattern.matcher(jsonString);
        Matcher senderMatcher = senderPattern.matcher(jsonString);
        Matcher receiverMatcher = receiverPattern.matcher(jsonString);
        Matcher timestampMatcher = timestampPattern.matcher(jsonString);
        Matcher dataMatcher = dataPattern.matcher(jsonString);

        typeMatcher.find();
        senderMatcher.find();
        receiverMatcher.find();
        timestampMatcher.find();
        dataMatcher.find();

        messageType = typeMatcher.group(1);
        sender = senderMatcher.group(1);
        receiver = receiverMatcher.group(1);
        timestamp = Integer.parseInt(timestampMatcher.group(1));
        data = dataMatcher.group(1);
    }

    public String toJson() {
        /* Método para transformar a mensagem em string JSON */
        
        return String.format(
            "{\"messageType\": \"%s\", \"sender\": \"%s\", \"receiver\": \"%s\", \"timestamp\": \"%s\", \"data\": \"%s\"}",
            messageType,
            sender,
            receiver,
            timestamp,
            data
        );
    }
}