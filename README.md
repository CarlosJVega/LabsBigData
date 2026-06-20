# Lab 5 - Crypto Lakehouse Bundle

Este proyecto es una demostracion guiada para pasar de notebooks manuales a un proyecto reproducible en Databricks usando Declarative Automation Bundles.

## Estructura

```text
crypto_lakehouse_bundle/
├── databricks.yml
├── resources/
│   └── jobs.yml
└── notebooks/
    └── 01_crypto_summary.py
```

## Requisitos

- Databricks CLI instalada y autenticada.
- Acceso al workspace de Databricks Free Edition.
- Tabla fuente disponible, por defecto:

```text
workspace.bigdata.crypto_prices_lab3_gold
```

## Comandos principales

```bash
databricks bundle validate
```

```bash
databricks bundle deploy
```

```bash
databricks bundle run crypto_summary_job
```

## Tabla de salida

```text
workspace.bigdata.crypto_bundle_summary
```
