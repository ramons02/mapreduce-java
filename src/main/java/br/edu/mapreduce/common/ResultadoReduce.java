package br.edu.mapreduce.common;

import java.util.List;

/**
 * Resposta de um Worker a uma TarefaReduce.
 */
public class ResultadoReduce implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final int particaoId;
    private final String workerId;
    private final List<KeyValue> saida;

    public ResultadoReduce(int particaoId, String workerId, List<KeyValue> saida) {
        this.particaoId = particaoId;
        this.workerId = workerId;
        this.saida = saida;
    }

    public int getParticaoId() {
        return particaoId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public List<KeyValue> getSaida() {
        return saida;
    }
}
