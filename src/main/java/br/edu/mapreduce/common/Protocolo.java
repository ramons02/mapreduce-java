package br.edu.mapreduce.common;

/**
 * Constantes compartilhadas por Master, Workers e Client.
 */
public final class Protocolo {

    /** Porta padrão em que o Master aceita conexões de Workers e Clients. */
    public static final int PORTA_MASTER = 5000;

    /** Host padrão do Master. */
    public static final String HOST_MASTER = "localhost";

    private Protocolo() {
    }
}
