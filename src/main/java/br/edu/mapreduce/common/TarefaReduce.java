package br.edu.mapreduce.common;

import java.util.List;
import java.util.Map;

/**
 * Tarefa de Reduce enviada pelo Master a um Worker.
 * Contém todos os valores de uma partição, já agrupados por chave.
 */
public class TarefaReduce implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final int particaoId;
    private final Map<String, List<String>> agrupado;
    private final Reducer reducer;

    public TarefaReduce(int particaoId, Map<String, List<String>> agrupado, Reducer reducer) {
        this.particaoId = particaoId;
        this.agrupado = agrupado;
        this.reducer = reducer;
    }

    public int getParticaoId() {
        return particaoId;
    }

    public Map<String, List<String>> getAgrupado() {
        return agrupado;
    }

    public Reducer getReducer() {
        return reducer;
    }
}
