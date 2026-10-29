package br.edu.mapreduce.common;

/**
 * Primeira mensagem enviada por um Worker ao se conectar no Master.
 */
public class RegistroWorker implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final String workerId;

    public RegistroWorker(String workerId) {
        this.workerId = workerId;
    }

    public String getWorkerId() {
        return workerId;
    }
}
