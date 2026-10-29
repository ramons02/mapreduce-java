package br.edu.mapreduce.worker;

import br.edu.mapreduce.common.Mensagem;
import br.edu.mapreduce.common.Protocolo;
import br.edu.mapreduce.common.RegistroWorker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Nó de processamento do cluster.
 *
 * Conecta-se ao Master, registra-se e entra em um laço:
 * recebe uma tarefa (Map ou Reduce), executa e devolve o resultado.
 *
 * Uso: java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker [id] [hostMaster] [portaMaster]
 */
public class Worker {

    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String id;
    private final String host;
    private final int porta;
    private final ExecutorTarefas executor;

    public Worker(String id, String host, int porta) {
        this.id = id;
        this.host = host;
        this.porta = porta;
        this.executor = new ExecutorTarefas(id, this::log);
    }

    public static void main(String[] args) throws Exception {
        String id = args.length > 0 ? args[0] : "worker-" + UUID.randomUUID().toString().substring(0, 4);
        String host = args.length > 1 ? args[1] : Protocolo.HOST_MASTER;
        int porta = args.length > 2 ? Integer.parseInt(args[2]) : Protocolo.PORTA_MASTER;
        new Worker(id, host, porta).iniciar();
    }

    public void iniciar() throws IOException, ClassNotFoundException {
        log("Conectando ao Master em " + host + ":" + porta + "...");
        try (Socket socket = new Socket(host, porta)) {
            ObjectOutputStream saida = new ObjectOutputStream(socket.getOutputStream());
            saida.flush();
            ObjectInputStream entrada = new ObjectInputStream(socket.getInputStream());

            saida.writeObject(new RegistroWorker(id));
            saida.flush();
            log("Registrado no Master como '" + id + "'. Aguardando tarefas...");

            while (true) {
                Object recebido = entrada.readObject();
                if (!(recebido instanceof Mensagem)) {
                    log("Mensagem desconhecida ignorada: " + recebido.getClass().getName());
                    continue;
                }
                Mensagem resposta = executor.executar((Mensagem) recebido);
                saida.writeObject(resposta);
                saida.flush();
                saida.reset();
            }
        } catch (IOException e) {
            log("Conexão com o Master encerrada: " + e.getMessage());
        }
    }

    private void log(String mensagem) {
        System.out.println("[" + LocalTime.now().format(HORA) + "] [" + id.toUpperCase() + "] " + mensagem);
    }
}
