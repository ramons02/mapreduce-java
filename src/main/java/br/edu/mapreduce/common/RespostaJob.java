package br.edu.mapreduce.common;

import java.util.List;

/**
 * Resposta final do Master ao Client, com o resultado consolidado
 * e um log do que aconteceu em cada fase.
 */
public class RespostaJob implements Mensagem {

    private static final long serialVersionUID = 1L;

    private final boolean sucesso;
    private final String erro;
    private final List<KeyValue> resultado;
    private final List<String> log;
    private final long duracaoMs;

    public RespostaJob(boolean sucesso, String erro, List<KeyValue> resultado, List<String> log, long duracaoMs) {
        this.sucesso = sucesso;
        this.erro = erro;
        this.resultado = resultado;
        this.log = log;
        this.duracaoMs = duracaoMs;
    }

    public boolean isSucesso() {
        return sucesso;
    }

    public String getErro() {
        return erro;
    }

    public List<KeyValue> getResultado() {
        return resultado;
    }

    public List<String> getLog() {
        return log;
    }

    public long getDuracaoMs() {
        return duracaoMs;
    }
}
