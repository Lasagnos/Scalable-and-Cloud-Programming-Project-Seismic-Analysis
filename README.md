# Scalable-and-Cloud-Programming-Project-Seismic-Analysis
Progetto di Scalable and Cloud Programming per il corso di Informatica Magistrane dell'Università di Bologna, a.2025/2026.

Il file di logica principale è `src/main/scala/EarthquakeAnalysis.scala`

### Istruzioni per l'esecuzione
Assicurati di avere installato SBT. Dalla root del progetto, apri il terminale (o la SBT Shell in IntelliJ) ed esegui
```bash
sbt assembly
```
Verrà generato il file: `target/scala-2.12/EarthquakeAnalysis-assembly-1.0.jar`

Carica questo file .jar in un bucket Google Cloud Storage, assieme al dataset da analizzare.
Crea un cluster Dataproc (o usarne uno già presente), e infine eseguire il seguente comando:
```bash
gcloud dataproc jobs submit spark \
  --cluster=<nome> \
  --region=<regione> \
  --jar=gs://<bucket>/EarthquakeAnalysis-cache-1.0.jar \
  -- gs://<bucket>/<dataset>.csv gs://<bucket>/Outputs <num_partizioni>
```
Argomenti Posizionali:
1. path_input: Percorso del file CSV del dataset.
2. path_output: Percorso della directory di output (verrà creata automaticamente).
3. num_partizioni: (Opzionale) Numero di partizioni per il repartitioning (consigliato: 512-1024, default 512).
