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

A ordem de subida importa: **Master primeiro, Workers depois, Client por último**.
Abra terminais separados (um por nó) na raiz do projeto e deixe as janelas do Master
e dos Workers abertas enquanto o job roda.

### Windows (PowerShell)

Em cada terminal, execute antes `chcp 65001` para os acentos saírem corretos no console.

> **Atenção:** no PowerShell as opções `-D...` do Java precisam ficar **entre aspas**.
> Sem as aspas, o PowerShell quebra `-Dstdout.encoding=UTF-8` no ponto e o Java falha com
> `Could not find or load main class .encoding=UTF-8`.

**Terminal 1, Master:**

```powershell
java "-Dstdout.encoding=UTF-8" "-Dfile.encoding=UTF-8" -cp target\mapreduce.jar br.edu.mapreduce.master.Master
```

**Terminais 2, 3 e 4, Workers (um comando por terminal):**

```powershell
java "-Dstdout.encoding=UTF-8" "-Dfile.encoding=UTF-8" -cp target\mapreduce.jar br.edu.mapreduce.worker.Worker worker-1
java "-Dstdout.encoding=UTF-8" "-Dfile.encoding=UTF-8" -cp target\mapreduce.jar br.edu.mapreduce.worker.Worker worker-2
java "-Dstdout.encoding=UTF-8" "-Dfile.encoding=UTF-8" -cp target\mapreduce.jar br.edu.mapreduce.worker.Worker worker-3
```

**Terminal 5, Client (submete o job):**

```powershell
java "-Dstdout.encoding=UTF-8" "-Dfile.encoding=UTF-8" -cp target\mapreduce.jar br.edu.mapreduce.client.Client data\entrada.txt 2
```

### Linux / macOS

**Terminal 1, Master:**

```bash
java -cp target/mapreduce.jar br.edu.mapreduce.master.Master
```

**Terminais 2, 3 e 4, Workers (um comando por terminal):**

```bash
java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker worker-1
java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker worker-2
java -cp target/mapreduce.jar br.edu.mapreduce.worker.Worker worker-3
```

**Terminal 5, Client (submete o job):**

```bash
java -cp target/mapreduce.jar br.edu.mapreduce.client.Client data/entrada.txt 2
```

### O que esperar

O segundo argumento do Client é o número de reducers. O Client imprime o log das fases
executadas pelo Master e as palavras mais frequentes do arquivo.
O resultado completo (todas as palavras, ordenado) também é salvo em `saida.txt`.
Enquanto o job roda, as janelas dos Workers mostram as tarefas de map e reduce recebidas.

### Atalho

- Windows: `.\scripts\demo.ps1 3` abre o Master e 3 Workers em janelas separadas, já com
  a codificação configurada. Depois, basta rodar o Client no seu terminal.
  Um segundo argumento (`.\scripts\demo.ps1 3 2`) faz o worker-2 simular uma queda no meio do job.
  Se o PowerShell bloquear o script, rode antes `Set-ExecutionPolicy -Scope Process Bypass`.
- Linux/macOS: `./scripts/demo.sh 3`.

### Problemas comuns

- `Could not find or load main class .encoding=UTF-8`: faltaram as aspas nas opções `-D`
  no PowerShell (veja acima). Alternativa: omita as opções `-D` e rode só
  `java -cp target\mapreduce.jar ...` (os acentos podem sair trocados no console).
- `Address already in use` no Master: a porta 5000 já está ocupada. Feche o outro processo
  ou suba o Master em outra porta (`Master 6000`).
- `Connection refused` no Worker ou Client: o Master não está no ar ou está em outra porta.
- Client fica esperando sem resposta: nenhum Worker conectado. Suba ao menos um Worker.

### Parâmetros úteis

- `Worker [id] [hostMaster] [portaMaster]`: permite rodar Workers em outras máquinas da rede,
  apontando para o IP do Master.
- `Client [arquivo] [numReducers] [hostMaster] [portaMaster]`.
- `-Dmapreduce.atraso=0` no Worker remove o atraso artificial usado para a demonstração.
- `-Dmapreduce.cair.apos=N` no Worker simula uma queda: após concluir N tarefas, o processo
  encerra ao receber a próxima, sem responder.

### Demonstrando a tolerância a falhas

O roteiro completo da apresentação, com as saídas reais comentadas linha a linha, está em
[`docs/ROTEIRO-APRESENTACAO.md`](docs/ROTEIRO-APRESENTACAO.md).

**Queda no meio do job (recomendado).** O Worker aceita `-Dmapreduce.cair.apos=N`: ele executa
N tarefas normalmente e, ao receber a seguinte, encerra o processo sem responder. O script de
demonstração já faz isso pelo segundo argumento:

```powershell
.\scripts\demo.ps1 3 2      # worker-2 cai ao receber sua 2a tarefa
./scripts/demo.sh 3 2       # Linux/macOS
```

Rode o Client e observe no log do Master:

```
>>> FALHA: Worker worker-2 parou de responder (Connection reset). Removido do cluster. Redistribuindo tarefa MAP #4. Workers restantes: [worker-1, worker-3]
[MAP #4] enviada para worker-3
```

O resultado final é idêntico ao de um cluster saudável (mesmas 229 palavras distintas e
465 no total para `data/entrada.txt`); só o tempo do job aumenta.

**Queda entre jobs.** Alternativamente, suba 3 Workers, rode o Client, feche a janela de um
Worker e rode o Client de novo. O Master só percebe a queda ao tentar enviar a próxima tarefa,
por isso o job ainda começa listando os três Workers e a linha `>>> FALHA` aparece logo depois.

## Casos de uso do modelo no mercado

- Indexação de páginas web (origem do modelo, Google).
- Processamento de logs de servidores e análise de cliques.
- ETL de grandes volumes de dados em data lakes (Hadoop, Spark, AWS EMR, Google Dataproc).
- Sistemas de recomendação e cálculo de métricas agregadas em larga escala.

## Referência

Dean, J.; Ghemawat, S. *MapReduce: Simplified Data Processing on Large Clusters*. OSDI, 2004.
