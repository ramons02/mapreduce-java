package br.edu.mapreduce.common;

import java.io.Serializable;

/**
 * Função Map definida pelo usuário.
 * Recebe uma linha da entrada e emite zero ou mais pares intermediários.
 *
 * Implementações precisam ser Serializable porque são enviadas
 * pela rede do Master para os Workers.
 */
public interface Mapper extends Serializable {
    void map(String linha, Emitter saida);
}
