package br.edu.mapreduce.client;

import br.edu.mapreduce.common.KeyValue;
import br.edu.mapreduce.common.Protocolo;
import br.edu.mapreduce.common.RequisicaoJob;
import br.edu.mapreduce.common.RespostaJob;
import br.edu.mapreduce.jobs.ContadorPalavrasMapper;
import br.edu.mapreduce.jobs.ContadorPalavrasReducer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Cliente que submete um job de contagem de palavras ao Master
 * e exibe o resultado consolidado.
 *
 * Uso: java -cp target/mapreduce.jar br.edu.mapreduce.client.Client [arquivo] [numReducers] [hostMaster] [portaMaster]
 */
public class Client {

    private static final String ARQUIVO_SAIDA = "saida.txt";

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Path arquivo = Path.of(args.length > 0 ? args[0] : "data/entrada.txt");
        int numReducers = args.length > 1 ? Integer.parseInt(args[1]) : 2;
        String host = args.length > 2 ? args[2] : Protocolo.HOST_MASTER;
        int porta = args.length > 3 ? Integer.parseInt(args[3]) : Protocolo.PORTA_MASTER;

        List<String> linhas = Files.readAllLines(arquivo, StandardCharsets.UTF_8);
        System.out.println("=== CLIENT MAPREDUCE ===");
        System.out.println("Arquivo de entrada : " + arquivo + " (" + linhas.size() + " linhas)");
        System.out.println("Reducers           : " + numReducers);
        System.out.println("Master             : " + host + ":" + porta);
        System.out.println();

        RequisicaoJob requisicao = new RequisicaoJob(
                "contador-de-palavras",
                linhas,
                new ContadorPalavrasMapper(),
                new ContadorPalavrasReducer(),
                numReducers);

        RespostaJob resposta = submeter(host, porta, requisicao);

        System.out.println("--- Log do Master ---");
        for (String l : resposta.getLog()) {
            System.out.println("  " + l);
        }
        System.out.println();

        if (!resposta.isSucesso()) {
            System.out.println("Job FALHOU: " + resposta.getErro());
            System.exit(1);
        }

        imprimirResultado(resposta);
        salvarResultado(resposta, Path.of(ARQUIVO_SAIDA));
    }

    private static RespostaJob submeter(String host, int porta, RequisicaoJob requisicao)
            throws IOException, ClassNotFoundException {
        try (Socket socket = new Socket(host, porta)) {
            ObjectOutputStream saida = new ObjectOutputStream(socket.getOutputStream());
            saida.flush();
            ObjectInputStream entrada = new ObjectInputStream(socket.getInputStream());

            saida.writeObject(requisicao);
            saida.flush();
            System.out.println("Job submetido. Aguardando o cluster...");
            System.out.println();

            return (RespostaJob) entrada.readObject();
        }
    }

    private static List<KeyValue> ordenar(RespostaJob resposta) {
        List<KeyValue> resultado = new ArrayList<>(resposta.getResultado());
        resultado.sort(Comparator
                .comparingLong((KeyValue kv) -> Long.parseLong(kv.getValor())).reversed()
                .thenComparing(KeyValue::getChave));
        return resultado;
    }

    private static void imprimirResultado(RespostaJob resposta) {
        List<KeyValue> resultado = ordenar(resposta);

        long totalPalavras = resultado.stream().mapToLong(kv -> Long.parseLong(kv.getValor())).sum();

        System.out.println("--- Resultado: 20 palavras mais frequentes ---");
        System.out.printf("%-20s %s%n", "PALAVRA", "OCORRÊNCIAS");
        resultado.stream().limit(20)
                .forEach(kv -> System.out.printf("%-20s %s%n", kv.getChave(), kv.getValor()));
        System.out.println();
        System.out.println("Palavras distintas : " + resultado.size());
        System.out.println("Total de palavras  : " + totalPalavras);
        System.out.println("Tempo do job       : " + resposta.getDuracaoMs() + " ms");
    }

    private static void salvarResultado(RespostaJob resposta, Path destino) throws IOException {
        List<String> linhas = new ArrayList<>();
        for (KeyValue kv : ordenar(resposta)) {
            linhas.add(kv.getChave() + "\t" + kv.getValor());
        }
        Files.write(destino, linhas, StandardCharsets.UTF_8);
        System.out.println("Resultado completo salvo em " + destino.toAbsolutePath());
    }
}
