package br.edu.mapreduce.common;

import java.io.Serializable;

/**
 * Tipo base de todas as mensagens trocadas entre os nós.
 * Toda comunicação é feita por serialização Java sobre TCP.
 */
public interface Mensagem extends Serializable {
}
