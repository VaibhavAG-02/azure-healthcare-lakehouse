# Sample data

Place a small sample (a few hundred rows per table) of your Synthea CSV output here so the pipeline logic can be tested locally without any Azure dependency.

Generate the full dataset with:

```
synthea -p 1000 --exporter.csv.export=true --exporter.fhir.export=false --exporter.years_of_history=10 --exporter.baseDirectory=./output_1k Massachusetts
```

Copy a subset of `output_1k/csv/*.csv` here (e.g. `head -n 200 patients.csv > data/sample/patients.csv`, repeated per table, keeping the header row).
