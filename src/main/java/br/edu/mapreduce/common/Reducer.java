package br.edu.mapreduce.common;

import java.io.Serializable;
import java.util.List;

/**
 * Função Reduce definida pelo usuário.
 * Recebe uma chave e todos os valores intermediários associados a ela
 * e emite o resultado final.
 */
public interface Reducer extends Serializable {
    void reduce(String chave, List<String> valores, Emitter saida);
}
