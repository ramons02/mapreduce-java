package br.edu.mapreduce.common;

import java.util.List;
import java.util.Map;

/**
 * Resposta de um Worker a uma TarefaMap.
 * Os pares intermediários já vêm particionados por reducer
 * (índice da partição = hash(chave) mod numReducers).
 */
public class ResultadoMap implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final int tarefaId;
    private final String workerId;
    private final Map<Integer, List<KeyValue>> particoes;

    public ResultadoMap(int tarefaId, String workerId, Map<Integer, List<KeyValue>> particoes) {
        this.tarefaId = tarefaId;
        this.workerId = workerId;
        this.particoes = particoes;
    }

    public int getTarefaId() {
        return tarefaId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public Map<Integer, List<KeyValue>> getParticoes() {
        return particoes;
    }
}
