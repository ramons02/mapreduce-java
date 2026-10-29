package br.edu.mapreduce.jobs;

import br.edu.mapreduce.common.Emitter;
import br.edu.mapreduce.common.Mapper;

import java.util.Locale;

/**
 * Função Map do contador de palavras (o "hello world" do MapReduce).
 * Para cada palavra da linha emite o par (palavra, "1").
 */
public class ContadorPalavrasMapper implements Mapper {

    private static final long serialVersionUID = 1L;

    @Override
    public void map(String linha, Emitter saida) {
        String[] palavras = linha.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}]+");
        for (String palavra : palavras) {
            if (!palavra.isBlank()) {
                saida.emit(palavra, "1");
            }
        }
    }
}
