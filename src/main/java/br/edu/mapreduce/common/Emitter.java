package br.edu.mapreduce.common;

/**
 * Canal de saída usado pelas funções map e reduce para emitir pares chave-valor.
 */
public interface Emitter {
    void emit(String chave, String valor);
}
