package br.edu.mapreduce.worker;

import br.edu.mapreduce.common.Emitter;
import br.edu.mapreduce.common.KeyValue;
import br.edu.mapreduce.common.Mensagem;
import br.edu.mapreduce.common.ResultadoMap;
import br.edu.mapreduce.common.ResultadoReduce;
import br.edu.mapreduce.common.TarefaMap;
import br.edu.mapreduce.common.TarefaReduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Executa localmente as tarefas recebidas pelo Worker.
 *
 * Map:    aplica a função map a cada linha do split e particiona os pares
 *         emitidos por hash(chave) mod numReducers.
 * Reduce: aplica a função reduce a cada chave da partição recebida.
 */
public class ExecutorTarefas {

    /** Atraso artificial (ms) por tarefa, para a demonstração ficar visível. */
    private static final long ATRASO_DEMO_MS = Long.getLong("mapreduce.atraso", 400L);

    private final String workerId;
    private final Consumer<String> logger;

    public ExecutorTarefas(String workerId, Consumer<String> logger) {
        this.workerId = workerId;
        this.logger = logger;
    }

    public Mensagem executar(Mensagem tarefa) {
        if (tarefa instanceof TarefaMap) {
            return executarMap((TarefaMap) tarefa);
        }
        if (tarefa instanceof TarefaReduce) {
            return executarReduce((TarefaReduce) tarefa);
        }
        throw new IllegalArgumentException("Tarefa não suportada: " + tarefa.getClass().getName());
    }

    private ResultadoMap executarMap(TarefaMap tarefa) {
        logger.accept("MAP #" + tarefa.getTarefaId() + " recebido com " + tarefa.getLinhas().size() + " linhas");
        simularProcessamento();

        ColetorParticionado coletor = new ColetorParticionado(tarefa.getNumReducers());
        for (String linha : tarefa.getLinhas()) {
            tarefa.getMapper().map(linha, coletor);
        }

        logger.accept("MAP #" + tarefa.getTarefaId() + " concluído: " + coletor.total()
                + " pares em " + tarefa.getNumReducers() + " partições");
        return new ResultadoMap(tarefa.getTarefaId(), workerId, coletor.particoes);
    }

    private ResultadoReduce executarReduce(TarefaReduce tarefa) {
        logger.accept("REDUCE partição " + tarefa.getParticaoId() + " recebido com "
                + tarefa.getAgrupado().size() + " chaves");
        simularProcessamento();

        ColetorLista coletor = new ColetorLista();
        for (Map.Entry<String, List<String>> e : tarefa.getAgrupado().entrySet()) {
            tarefa.getReducer().reduce(e.getKey(), e.getValue(), coletor);
        }

        logger.accept("REDUCE partição " + tarefa.getParticaoId() + " concluído: " + coletor.saida.size() + " chaves");
        return new ResultadoReduce(tarefa.getParticaoId(), workerId, coletor.saida);
    }

    private void simularProcessamento() {
        if (ATRASO_DEMO_MS <= 0) {
            return;
        }
        try {
            Thread.sleep(ATRASO_DEMO_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Emitter da fase Map: separa os pares por partição de destino. */
    private static final class ColetorParticionado implements Emitter {
        private final int numReducers;
        private final Map<Integer, List<KeyValue>> particoes = new HashMap<>();

        ColetorParticionado(int numReducers) {
            this.numReducers = numReducers;
        }

        @Override
        public void emit(String chave, String valor) {
            int particao = (chave.hashCode() & 0x7fffffff) % numReducers;
            particoes.computeIfAbsent(particao, p -> new ArrayList<>()).add(new KeyValue(chave, valor));
        }

        int total() {
            return particoes.values().stream().mapToInt(List::size).sum();
        }
    }

    /** Emitter da fase Reduce: acumula a saída final em lista. */
    private static final class ColetorLista implements Emitter {
        private final List<KeyValue> saida = new ArrayList<>();

        @Override
        public void emit(String chave, String valor) {
            saida.add(new KeyValue(chave, valor));
        }
    }
}
