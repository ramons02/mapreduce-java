# MapReduce Distribuído em Java

Implementação didática do modelo de programação distribuída **MapReduce**, feita em Java puro
(sem Hadoop), com comunicação entre os nós por **sockets TCP** e serialização de objetos.

Trabalho da disciplina de Programação Distribuída.

**Integrantes:** Ramon Silva, Guilherme Silva, Lucio Aguiar

## Arquitetura

```
                       +----------------+
                       |     CLIENT     |  lê o arquivo, submete o job
                       +--------+-------+  e exibe o resultado
                                | RequisicaoJob / RespostaJob
                                v
                       +----------------+
                       |     MASTER     |  split, escalonamento, shuffle,
                       |  (Coordenador) |  tolerância a falhas, consolidação
                       +--+-----+-----+-+
          TarefaMap /     |     |     |     TarefaReduce /
          ResultadoMap    |     |     |     ResultadoReduce
                          v     v     v
                    +------+ +------+ +------+
                    |WORKER| |WORKER| |WORKER|   executam map() e reduce()
                    |  1   | |  2   | |  3   |
                    +------+ +------+ +------+
```

### Fluxo de um job

1. **Submissão**: o Client lê o arquivo de entrada e envia ao Master as linhas, a função `map`,
   a função `reduce` e o número de reducers (partições).
2. **Split**: o Master divide as linhas em fatias (uma por tarefa de map).
3. **Map**: cada fatia vira uma `TarefaMap`, distribuída em round-robin entre os Workers.
   O Worker aplica `map()` a cada linha e devolve os pares intermediários já particionados
   por `hash(chave) mod numReducers`.
4. **Shuffle & Sort**: o Master agrupa os pares de todos os maps por partição e por chave
   (ordenadas).
5. **Reduce**: cada partição vira uma `TarefaReduce`, enviada a um Worker, que aplica
   `reduce()` por chave e devolve a saída.
6. **Resultado**: o Master consolida as saídas, ordena e responde ao Client.

### Tolerância a falhas

Se um Worker cair durante uma tarefa, o Master detecta a queda da conexão, remove o Worker
do registro e reenvia a tarefa para outro Worker vivo.

## Estrutura do código

| Pacote                      | Responsável  | Conteúdo                                                        |
|-----------------------------|--------------|-----------------------------------------------------------------|
| `br.edu.mapreduce.common`   | Ramon        | Interfaces `Mapper`, `Reducer`, `Emitter`, `KeyValue` e mensagens do protocolo |
| `br.edu.mapreduce.master`   | Guilherme    | `Master`, `Coordenador` (split, map, shuffle, reduce), `RegistroDeWorkers`, `ConexaoWorker`, `Divisor` |
| `br.edu.mapreduce.worker`   | Lucio        | `Worker` (conexão e laço de tarefas) e `ExecutorTarefas` (execução de map/reduce) |
| `br.edu.mapreduce.client`   | Ramon        | `Client` que submete o job e imprime o resultado                 |
| `br.edu.mapreduce.jobs`     | Ramon        | Job de exemplo: `ContadorPalavrasMapper` e `ContadorPalavrasReducer` |
| `data/`, `scripts/`         | Lucio        | Arquivo de entrada e scripts da demonstração                     |

## Requisitos

- Java 17 ou superior
- Maven 3.8 ou superior

## Como compilar

```bash
mvn -q package
```

Gera `target/mapreduce.jar`.

## Como executar a demonstração

Abra terminais separados (um por nó) na raiz do projeto.

**Terminal 1, Master:**

```bash
java -cp target/mapreduce.jar br.edu.mapreduce.master.Master
```

**Terminais 2, 3 e 4, Workers:**

```bash
java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker worker-1
java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker worker-2
java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker worker-3
```

**Terminal 5, Client (submete o job):**

```bash
java -cp target/mapreduce.jar br.edu.mapreduce.client.Client data/entrada.txt 2
```

O segundo argumento é o número de reducers. O Client imprime o log das fases executadas
pelo Master e as palavras mais frequentes do arquivo.

### Atalho

- Windows: `.\scripts\demo.ps1 3` abre o Master e 3 Workers em janelas separadas.
- Linux/macOS: `./scripts/demo.sh 3`.

No Windows, para acentos corretos no console, use `chcp 65001` e a opção
`-Dstdout.encoding=UTF-8` ao executar o `java` (os scripts já fazem isso).

### Parâmetros úteis

- `Worker [id] [hostMaster] [portaMaster]`: permite rodar Workers em outras máquinas da rede,
  apontando para o IP do Master.
- `Client [arquivo] [numReducers] [hostMaster] [portaMaster]`.
- `-Dmapreduce.atraso=0` no Worker remove o atraso artificial usado para a demonstração.

### Demonstrando a tolerância a falhas

1. Suba o Master e 3 Workers.
2. Rode o Client uma vez e observe as tarefas distribuídas entre os três.
3. Feche a janela de um dos Workers.
4. Rode o Client novamente: o Master registra a falha, remove o Worker e redistribui a tarefa.

## Casos de uso do modelo no mercado

- Indexação de páginas web (origem do modelo, Google).
- Processamento de logs de servidores e análise de cliques.
- ETL de grandes volumes de dados em data lakes (Hadoop, Spark, AWS EMR, Google Dataproc).
- Sistemas de recomendação e cálculo de métricas agregadas em larga escala.

## Referência

Dean, J.; Ghemawat, S. *MapReduce: Simplified Data Processing on Large Clusters*. OSDI, 2004.
