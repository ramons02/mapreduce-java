package br.edu.mapreduce.master;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Lista thread-safe dos Workers atualmente conectados ao Master.
 */
public class RegistroDeWorkers {

    private final List<ConexaoWorker> workers = new CopyOnWriteArrayList<>();

    public void adicionar(ConexaoWorker worker) {
        workers.add(worker);
    }

    public void remover(ConexaoWorker worker) {
        workers.remove(worker);
        worker.fechar();
    }

    /** Cópia da lista de workers vivos no momento da chamada. */
    public List<ConexaoWorker> ativos() {
        return new ArrayList<>(workers);
    }

    public int quantidade() {
        return workers.size();
    }
}
