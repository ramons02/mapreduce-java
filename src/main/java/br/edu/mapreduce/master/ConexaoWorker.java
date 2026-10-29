package br.edu.mapreduce.master;

import br.edu.mapreduce.common.Mensagem;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Canal de comunicação do Master com um único Worker.
 *
 * O protocolo é requisição-resposta: o Master envia uma tarefa e aguarda
 * o resultado na mesma conexão. O método é sincronizado para que o mesmo
 * Worker nunca receba duas tarefas simultaneamente.
 */
public class ConexaoWorker {

    private final String workerId;
    private final Socket socket;
    private final ObjectInputStream entrada;
    private final ObjectOutputStream saida;

    public ConexaoWorker(String workerId, Socket socket, ObjectInputStream entrada, ObjectOutputStream saida) {
        this.workerId = workerId;
        this.socket = socket;
        this.entrada = entrada;
        this.saida = saida;
    }

    public String getWorkerId() {
        return workerId;
    }

    /**
     * Envia uma tarefa ao Worker e bloqueia até receber a resposta.
     *
     * @throws IOException se o Worker caiu ou a conexão foi perdida
     */
    public synchronized Mensagem executar(Mensagem tarefa) throws IOException {
        try {
            saida.writeObject(tarefa);
            saida.flush();
            // Evita que o cache de referências do ObjectOutputStream cresça sem limite.
            saida.reset();
            return (Mensagem) entrada.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Resposta inválida do worker " + workerId, e);
        }
    }

    public void fechar() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // conexão já estava fechada
        }
    }

    @Override
    public String toString() {
        return workerId;
    }
}
