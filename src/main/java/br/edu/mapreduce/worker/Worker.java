package br.edu.mapreduce.worker;

import br.edu.mapreduce.common.Mensagem;
import br.edu.mapreduce.common.Protocolo;
import br.edu.mapreduce.common.RegistroWorker;
import br.edu.mapreduce.common.TarefaMap;
import br.edu.mapreduce.common.TarefaReduce;

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

    /**
     * Demonstração de tolerância a falhas: depois de concluir N tarefas, o Worker
     * encerra o processo ao receber a próxima, sem responder (simula uma queda).
     * Ative com -Dmapreduce.cair.apos=N. Zero (padrão) desativa.
     */
    private static final int CAIR_APOS = Integer.getInteger("mapreduce.cair.apos", 0);

    private final String id;
    private final String host;
    private final int porta;
    private final ExecutorTarefas executor;
    private int tarefasConcluidas = 0;

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
            if (CAIR_APOS > 0) {
                log("MODO DE FALHA SIMULADA: este Worker vai cair ao receber a "
                        + (CAIR_APOS + 1) + "a tarefa, sem responder.");
            }

            while (true) {
                Object recebido = entrada.readObject();
                if (!(recebido instanceof Mensagem)) {
                    log("Mensagem desconhecida ignorada: " + recebido.getClass().getName());
                    continue;
                }
                Mensagem tarefa = (Mensagem) recebido;
                if (CAIR_APOS > 0 && tarefasConcluidas >= CAIR_APOS) {
                    log("QUEDA SIMULADA: recebi " + descrever(tarefa)
                            + " e vou encerrar sem responder. O Master deve redistribuir esta tarefa.");
                    System.exit(1);
                }
                Mensagem resposta = executor.executar(tarefa);
                saida.writeObject(resposta);
                saida.flush();
                saida.reset();
                tarefasConcluidas++;
            }
        } catch (IOException e) {
            log("Conexão com o Master encerrada: " + e.getMessage());
        }
    }

    private static String descrever(Mensagem tarefa) {
        if (tarefa instanceof TarefaMap) {
            return "MAP #" + ((TarefaMap) tarefa).getTarefaId();
        }
        if (tarefa instanceof TarefaReduce) {
            return "REDUCE partição " + ((TarefaReduce) tarefa).getParticaoId();
        }
        return tarefa.getClass().getSimpleName();
    }

    private void log(String mensagem) {
        System.out.println("[" + LocalTime.now().format(HORA) + "] [" + id.toUpperCase() + "] " + mensagem);
    }
}
