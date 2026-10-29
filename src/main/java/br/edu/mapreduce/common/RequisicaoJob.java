package br.edu.mapreduce.common;

import java.util.List;

/**
 * Enviada pelo Client ao Master para submeter um job.
 * Carrega os dados de entrada, as funções map/reduce e o número de partições (reducers).
 */
public class RequisicaoJob implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final String nomeJob;
    private final List<String> linhas;
    private final Mapper mapper;
    private final Reducer reducer;
    private final int numReducers;

    public RequisicaoJob(String nomeJob, List<String> linhas, Mapper mapper, Reducer reducer, int numReducers) {
        this.nomeJob = nomeJob;
        this.linhas = linhas;
        this.mapper = mapper;
        this.reducer = reducer;
        this.numReducers = numReducers;
    }

    public String getNomeJob() {
        return nomeJob;
    }

    public List<String> getLinhas() {
        return linhas;
    }

    public Mapper getMapper() {
        return mapper;
    }

    public Reducer getReducer() {
        return reducer;
    }

    public int getNumReducers() {
        return numReducers;
    }
}
