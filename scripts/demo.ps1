# Sobe o cluster de demonstração no Windows: 1 Master + N Workers, cada um em sua janela.
# Uso (na raiz do projeto):  .\scripts\demo.ps1 [numeroDeWorkers]
param(
    [int]$Workers = 3
)

$raiz = Split-Path -Parent $PSScriptRoot
$jar = Join-Path $raiz "target\mapreduce.jar"

if (-not (Test-Path $jar)) {
    Write-Host "Compilando o projeto..."
    Push-Location $raiz
    mvn -q package
    Pop-Location
}

$javaOpts = "-Dstdout.encoding=UTF-8 -Dfile.encoding=UTF-8"

Write-Host "Iniciando Master..."
Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "chcp 65001 | Out-Null; `$host.UI.RawUI.WindowTitle = 'MASTER'; java $javaOpts -cp `"$jar`" br.edu.mapreduce.master.Master"

Start-Sleep -Seconds 2

for ($i = 1; $i -le $Workers; $i++) {
    $id = "worker-$i"
    Write-Host "Iniciando $id..."
    Start-Process powershell -ArgumentList "-NoExit", "-Command",
        "chcp 65001 | Out-Null; `$host.UI.RawUI.WindowTitle = '$id'; java $javaOpts -cp `"$jar`" br.edu.mapreduce.worker.Worker $id"
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "Cluster no ar. Para rodar o job, execute em outro terminal (na raiz do projeto):"
Write-Host "  java $javaOpts -cp target\mapreduce.jar br.edu.mapreduce.client.Client data\entrada.txt 2"
