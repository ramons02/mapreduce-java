package br.edu.mapreduce.master;

import br.edu.mapreduce.common.Mensagem;
import br.edu.mapreduce.common.Protocolo;
import br.edu.mapreduce.common.RegistroWorker;
import br.edu.mapreduce.common.RequisicaoJob;
import br.edu.mapreduce.common.RespostaJob;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nó coordenador do cluster MapReduce.
 *
 * Responsabilidades:
 *  - aceitar conexões de Workers e mantê-los registrados;
 *  - aceitar conexões de Clients que submetem jobs;
 *  - delegar a execução de cada job ao {@link Coordenador}.
 *
 * Uso: java -cp target/mapreduce.jar br.edu.mapreduce.master.Master [porta]
 */
public class Master {

    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final int porta;
    private final RegistroDeWorkers registro = new RegistroDeWorkers();

    public Master(int porta) {
        this.porta = porta;
    }

    public static void main(String[] args) throws IOException {
        int porta = args.length > 0 ? Integer.parseInt(args[0]) : Protocolo.PORTA_MASTER;
        new Master(porta).iniciar();
    }

    public void iniciar() throws IOException {
        try (ServerSocket servidor = new ServerSocket(porta)) {
            log("Master iniciado na porta " + porta + ". Aguardando Workers e Clients...");
            while (true) {
                Socket socket = servidor.accept();
                Thread t = new Thread(() -> tratarConexao(socket), "conexao-" + socket.getPort());
                t.setDaemon(true);
                t.start();
            }
        }
    }

    /**
     * Cada conexão começa com uma mensagem de apresentação:
     * RegistroWorker (é um Worker) ou RequisicaoJob (é um Client).
     */
    private void tratarConexao(Socket socket) {
        try {
            // A ordem importa: criar o ObjectOutputStream antes do ObjectInputStream
            // em ambos os lados evita deadlock na troca de cabeçalhos.
            ObjectOutputStream saida = new ObjectOutputStream(socket.getOutputStream());
            saida.flush();
            ObjectInputStream entrada = new ObjectInputStream(socket.getInputStream());

            Object primeira = entrada.readObject();

            if (primeira instanceof RegistroWorker) {
                RegistroWorker reg = (RegistroWorker) primeira;
                ConexaoWorker conexao = new ConexaoWorker(reg.getWorkerId(), socket, entrada, saida);
                registro.adicionar(conexao);
                log("Worker registrado: " + reg.getWorkerId()
                        + " (" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort() + ")"
                        + " | total de workers: " + registro.quantidade());
                // A conexão fica aberta; o Coordenador usa esta conexão para enviar tarefas.
                return;
            }

            if (primeira instanceof RequisicaoJob) {
                RequisicaoJob requisicao = (RequisicaoJob) primeira;
                log("Job recebido: '" + requisicao.getNomeJob() + "' com "
                        + requisicao.getLinhas().size() + " linhas e "
                        + requisicao.getNumReducers() + " reducers");

                Coordenador coordenador = new Coordenador(registro, Master::log);
                RespostaJob resposta = coordenador.executar(requisicao);

                enviar(saida, resposta);
                log("Job '" + requisicao.getNomeJob() + "' finalizado em " + resposta.getDuracaoMs() + " ms"
                        + (resposta.isSucesso() ? "" : " com erro: " + resposta.getErro()));
                socket.close();
                return;
            }

            log("Mensagem inicial desconhecida: " + primeira.getClass().getName());
            socket.close();

        } catch (IOException | ClassNotFoundException e) {
            log("Conexão encerrada: " + e.getMessage());
        }
    }

    private static void enviar(ObjectOutputStream saida, Mensagem mensagem) throws IOException {
        saida.writeObject(mensagem);
        saida.flush();
    }

    static void log(String mensagem) {
        System.out.println("[" + LocalTime.now().format(HORA) + "] [MASTER] " + mensagem);
    }
}
