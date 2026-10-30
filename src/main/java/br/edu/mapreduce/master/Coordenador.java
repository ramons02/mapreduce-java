package br.edu.mapreduce.master;

import br.edu.mapreduce.common.KeyValue;
import br.edu.mapreduce.common.Mensagem;
import br.edu.mapreduce.common.RequisicaoJob;
import br.edu.mapreduce.common.RespostaJob;
import br.edu.mapreduce.common.ResultadoMap;
import br.edu.mapreduce.common.ResultadoReduce;
import br.edu.mapreduce.common.TarefaMap;
import br.edu.mapreduce.common.TarefaReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Orquestra a execução de um job MapReduce:
 *
 *   1. Split    - divide a entrada em fatias;
 *   2. Map      - distribui as fatias entre os Workers e coleta pares intermediários;
 *   3. Shuffle  - agrupa os pares por partição e por chave (ordenados);
 *   4. Reduce   - envia cada partição a um Worker e coleta a saída final;
 *   5. Resultado consolidado e ordenado por chave.
 *
 * Se um Worker cair no meio de uma tarefa, a tarefa é reatribuída a outro Worker vivo.
 */
public class Coordenador {

    private final RegistroDeWorkers registro;
    private final Consumer<String> logger;
    // Sincronizada porque as tarefas de uma fase registram log em paralelo.
    private final List<String> logDoJob = Collections.synchronizedList(new ArrayList<>());

    public Coordenador(RegistroDeWorkers registro, Consumer<String> logger) {
        this.registro = registro;
        this.logger = logger;
    }

    public RespostaJob executar(RequisicaoJob job) {
        long inicio = System.currentTimeMillis();
        try {
            List<ConexaoWorker> workers = registro.ativos();
            if (workers.isEmpty()) {
                throw new IllegalStateException("Nenhum Worker registrado no Master.");
            }
            log("Workers disponíveis: " + workers);

            // ---------- 1. SPLIT ----------
            int numSplits = Math.min(job.getLinhas().size(), workers.size() * 2);
            List<List<String>> splits = Divisor.dividir(job.getLinhas(), numSplits);
            log("Split: " + job.getLinhas().size() + " linhas divididas em " + splits.size() + " fatias");

            // ---------- 2. MAP ----------
            List<ResultadoMap> resultadosMap = faseMap(splits, job);

            // ---------- 3. SHUFFLE & SORT ----------
            Map<Integer, Map<String, List<String>>> particoes = shuffle(resultadosMap, job.getNumReducers());

            // ---------- 4. REDUCE ----------
            List<ResultadoReduce> resultadosReduce = faseReduce(particoes, job);

            // ---------- 5. CONSOLIDAÇÃO ----------
            List<KeyValue> resultado = new ArrayList<>();
            for (ResultadoReduce r : resultadosReduce) {
                resultado.addAll(r.getSaida());
            }
            resultado.sort(Comparator.comparing(KeyValue::getChave));
            log("Resultado final: " + resultado.size() + " chaves");

            long duracao = System.currentTimeMillis() - inicio;
            return new RespostaJob(true, null, resultado, new ArrayList<>(logDoJob), duracao);

        } catch (Exception e) {
            log("Falha no job: " + e.getMessage());
            long duracao = System.currentTimeMillis() - inicio;
            return new RespostaJob(false, e.getMessage(), new ArrayList<>(), new ArrayList<>(logDoJob), duracao);
        }
    }

    private List<ResultadoMap> faseMap(List<List<String>> splits, RequisicaoJob job)
            throws InterruptedException, ExecutionException {
        log("Fase MAP iniciada com " + splits.size() + " tarefas");
        List<Mensagem> tarefas = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
            tarefas.add(new TarefaMap(i, splits.get(i), job.getMapper(), job.getNumReducers()));
        }
        List<Mensagem> respostas = distribuir(tarefas, "MAP");
        List<ResultadoMap> resultados = new ArrayList<>();
        for (Mensagem m : respostas) {
            ResultadoMap r = (ResultadoMap) m;
            int pares = r.getParticoes().values().stream().mapToInt(List::size).sum();
            log("  Map #" + r.getTarefaId() + " concluído por " + r.getWorkerId()
                    + " -> " + pares + " pares intermediários");
            resultados.add(r);
        }
        return resultados;
    }

    /**
     * Junta os pares intermediários de todos os maps, por partição e por chave.
     * TreeMap garante as chaves ordenadas, equivalente ao "sort" do Hadoop.
     */
    private Map<Integer, Map<String, List<String>>> shuffle(List<ResultadoMap> resultadosMap, int numReducers) {
        Map<Integer, Map<String, List<String>>> particoes = new TreeMap<>();
        for (int p = 0; p < numReducers; p++) {
            particoes.put(p, new TreeMap<>());
        }
        for (ResultadoMap r : resultadosMap) {
            for (Map.Entry<Integer, List<KeyValue>> e : r.getParticoes().entrySet()) {
                Map<String, List<String>> agrupado = particoes.get(e.getKey());
                for (KeyValue kv : e.getValue()) {
                    agrupado.computeIfAbsent(kv.getChave(), k -> new ArrayList<>()).add(kv.getValor());
                }
            }
        }
        for (Map.Entry<Integer, Map<String, List<String>>> e : particoes.entrySet()) {
            log("Shuffle: partição " + e.getKey() + " ficou com " + e.getValue().size() + " chaves distintas");
        }
        return particoes;
    }

    private List<ResultadoReduce> faseReduce(Map<Integer, Map<String, List<String>>> particoes, RequisicaoJob job)
            throws InterruptedException, ExecutionException {
        log("Fase REDUCE iniciada com " + particoes.size() + " tarefas");
        List<Mensagem> tarefas = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, List<String>>> e : particoes.entrySet()) {
            tarefas.add(new TarefaReduce(e.getKey(), e.getValue(), job.getReducer()));
        }
        List<Mensagem> respostas = distribuir(tarefas, "REDUCE");
        List<ResultadoReduce> resultados = new ArrayList<>();
        for (Mensagem m : respostas) {
            ResultadoReduce r = (ResultadoReduce) m;
            log("  Reduce partição " + r.getParticaoId() + " concluído por " + r.getWorkerId()
                    + " -> " + r.getSaida().size() + " chaves");
            resultados.add(r);
        }
        return resultados;
    }

    /**
     * Distribui as tarefas em round-robin entre os Workers e executa em paralelo.
     * Uma thread por tarefa; cada ConexaoWorker serializa as tarefas que recebe.
     */
    private List<Mensagem> distribuir(List<Mensagem> tarefas, String fase)
            throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, tarefas.size()));
        try {
            List<Future<Mensagem>> futuros = new ArrayList<>();
            for (int i = 0; i < tarefas.size(); i++) {
                final int indice = i;
                final Mensagem tarefa = tarefas.get(i);
                futuros.add(pool.submit(() -> executarComRetentativa(tarefa, indice, fase)));
            }
            List<Mensagem> respostas = new ArrayList<>();
            for (Future<Mensagem> f : futuros) {
                respostas.add(f.get());
            }
            return respostas;
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Tenta executar a tarefa no Worker preferido; se ele falhar,
     * remove-o do registro e tenta nos demais.
     */
    private Mensagem executarComRetentativa(Mensagem tarefa, int indice, String fase) throws IOException {
        IOException ultimoErro = null;
        int tentativas = 0;
        while (tentativas < 5) {
            List<ConexaoWorker> vivos = registro.ativos();
            if (vivos.isEmpty()) {
                break;
            }
            ConexaoWorker alvo = vivos.get((indice + tentativas) % vivos.size());
            try {
                log("  [" + fase + " #" + indice + "] enviada para " + alvo.getWorkerId());
                return alvo.executar(tarefa);
            } catch (IOException e) {
                ultimoErro = e;
                registro.remover(alvo);
                log("  >>> FALHA: Worker " + alvo.getWorkerId() + " parou de responder ("
                        + descreverErro(e) + "). Removido do cluster. Redistribuindo tarefa "
                        + fase + " #" + indice + ". Workers restantes: " + registro.ativos());
                tentativas++;
            }
        }
        throw new IOException("Não há Workers disponíveis para a tarefa " + fase + " #" + indice, ultimoErro);
    }

    /** EOFException vem sem mensagem; traduz para algo legível no log da demonstração. */
    private static String descreverErro(IOException e) {
        if (e.getMessage() == null || e instanceof java.io.EOFException) {
            return "conexão encerrada pelo worker";
        }
        return e.getMessage();
    }

    private void log(String mensagem) {
        logDoJob.add(mensagem);
        logger.accept(mensagem);
    }
}
