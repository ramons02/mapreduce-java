package br.edu.mapreduce.jobs;

import br.edu.mapreduce.common.Emitter;
import br.edu.mapreduce.common.Reducer;

import java.util.List;

/**
 * Função Reduce do contador de palavras: soma as ocorrências de cada palavra.
 */
public class ContadorPalavrasReducer implements Reducer {

    private static final long serialVersionUID = 1L;

    @Override
    public void reduce(String chave, List<String> valores, Emitter saida) {
        long soma = 0;
        for (String v : valores) {
            soma += Long.parseLong(v);
        }
        saida.emit(chave, String.valueOf(soma));
    }
}
