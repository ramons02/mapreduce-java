package br.edu.mapreduce.master;

import java.util.ArrayList;
import java.util.List;

/**
 * Fase de Split: divide a entrada em fatias de tamanho semelhante,
 * uma para cada tarefa de Map.
 */
public final class Divisor {

    private Divisor() {
    }

    public static List<List<String>> dividir(List<String> linhas, int numSplits) {
        List<List<String>> splits = new ArrayList<>();
        if (linhas.isEmpty()) {
            return splits;
        }
        int n = Math.max(1, Math.min(numSplits, linhas.size()));
        int tamanho = (int) Math.ceil((double) linhas.size() / n);
        for (int inicio = 0; inicio < linhas.size(); inicio += tamanho) {
            int fim = Math.min(inicio + tamanho, linhas.size());
            splits.add(new ArrayList<>(linhas.subList(inicio, fim)));
        }
        return splits;
    }
}
