package br.edu.mapreduce.common;

import java.util.List;

/**
 * Tarefa de Map enviada pelo Master a um Worker.
 * Contém um split (fatia) da entrada e a função map a aplicar.
 */
public class TarefaMap implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final int tarefaId;
    private final List<String> linhas;
    private final Mapper mapper;
    private final int numReducers;

    public TarefaMap(int tarefaId, List<String> linhas, Mapper mapper, int numReducers) {
        this.tarefaId = tarefaId;
        this.linhas = linhas;
        this.mapper = mapper;
        this.numReducers = numReducers;
    }

    public int getTarefaId() {
        return tarefaId;
    }

    public List<String> getLinhas() {
        return linhas;
    }

    public Mapper getMapper() {
        return mapper;
    }

    public int getNumReducers() {
        return numReducers;
    }
}
