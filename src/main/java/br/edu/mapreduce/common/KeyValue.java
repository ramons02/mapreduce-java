package br.edu.mapreduce.common;

import java.io.Serializable;
import java.util.Objects;

/**
 * Par chave-valor, unidade básica de dados que circula entre as fases
 * de Map, Shuffle e Reduce.
 */
public class KeyValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String chave;
    private final String valor;

    public KeyValue(String chave, String valor) {
        this.chave = chave;
        this.valor = valor;
    }

    public String getChave() {
        return chave;
    }

    public String getValor() {
        return valor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyValue)) return false;
        KeyValue that = (KeyValue) o;
        return chave.equals(that.chave) && Objects.equals(valor, that.valor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chave, valor);
    }

    @Override
    public String toString() {
        return chave + "=" + valor;
    }
}
