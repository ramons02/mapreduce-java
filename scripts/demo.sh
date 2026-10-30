#!/usr/bin/env bash
# Sobe o cluster de demonstração no Linux/macOS: 1 Master + N Workers.
# Uso (na raiz do projeto):  ./scripts/demo.sh [numeroDeWorkers] [workerQueCai]
#   workerQueCai: numero do Worker que simula uma queda no meio do job (0 = nenhum).
#   Ex.: ./scripts/demo.sh 3 2  -> worker-2 cai ao receber sua 2a tarefa.
set -e

WORKERS="${1:-3}"
WORKER_QUE_CAI="${2:-0}"
RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$RAIZ/target/mapreduce.jar"
JAVA_OPTS="-Dstdout.encoding=UTF-8 -Dfile.encoding=UTF-8"

if [ ! -f "$JAR" ]; then
  echo "Compilando o projeto..."
  (cd "$RAIZ" && mvn -q package)
fi

abrir_terminal() {
  local titulo="$1"
  local comando="$2"
  if command -v gnome-terminal >/dev/null 2>&1; then
    gnome-terminal --title="$titulo" -- bash -c "$comando; exec bash"
  elif command -v xterm >/dev/null 2>&1; then
    xterm -T "$titulo" -e bash -c "$comando; exec bash" &
  elif [ "$(uname)" = "Darwin" ]; then
    osascript -e "tell application \"Terminal\" to do script \"$comando\""
  else
    echo "Nenhum emulador de terminal encontrado; rodando $titulo em segundo plano."
    bash -c "$comando" &
  fi
}

echo "Iniciando Master..."
abrir_terminal "MASTER" "java $JAVA_OPTS -cp '$JAR' br.edu.mapreduce.master.Master"
sleep 2

for i in $(seq 1 "$WORKERS"); do
  OPTS="$JAVA_OPTS"
  if [ "$i" = "$WORKER_QUE_CAI" ]; then
    OPTS="$JAVA_OPTS -Dmapreduce.cair.apos=1"
    echo "Iniciando worker-$i (vai simular uma queda durante o job)..."
  else
    echo "Iniciando worker-$i..."
  fi
  abrir_terminal "worker-$i" "java $OPTS -cp '$JAR' br.edu.mapreduce.worker.Worker worker-$i"
  sleep 0.5
done

echo
echo "Cluster no ar. Para rodar o job, execute em outro terminal (na raiz do projeto):"
echo "  java $JAVA_OPTS -cp target/mapreduce.jar br.edu.mapreduce.client.Client data/entrada.txt 2"
