# Olist Lakehouse — Portafolio Databricks

Pipeline de datos end-to-end construido con Databricks, usando el dataset público [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (9 CSVs, ~100MB).

## Arquitectura

Implementa el patrón Medallion (Bronze / Silver / Gold) con:

- **Lakeflow Declarative Pipelines** para las transformaciones
- **Declarative Automation Bundles (DABs)** para infraestructura como código
- **GitHub Actions** para CI/CD con dos workspaces Free Edition (DEV y PROD)
- **Unity Catalog** con catálogo `olist` y schemas por capa (`bronze`, `silver`, `gold`)

## Estructura del repositorio

```
databricks.yml              # Configuracion del bundle (targets DEV/PROD)
resources/                  # Definiciones de pipeline y jobs
scripts/
  setup_catalogs.sql        # Setup inicial de Unity Catalog (una vez por workspace)
src/
  dbc_elt_olist_lakehouse/          # Codigo Python compartido
  dbc_elt_olist_lakehouse_etl/      # Transformaciones del pipeline
    transformations/
tests/                      # Tests con Databricks Connect
fixtures/                   # Datos de prueba
```

## Setup inicial

### 1. Prerequisitos

- Databricks CLI instalado y configurado (`~/.databrickscfg`)
- Perfil `dbc-dev` apuntando al workspace DEV
- Perfil `dbc-prod` apuntando al workspace PROD

### 2. Unity Catalog (una vez por workspace)

Ejecutar en la SQL Console del workspace correspondiente:

```bash
scripts/setup_catalogs.sql
```

Crea el catalogo `olist` con los schemas `bronze`, `silver` y `gold`.

### 3. Deploy en DEV

```bash
databricks bundle validate --profile dbc-dev
databricks bundle deploy --target development --profile dbc-dev
```

### 4. Deploy en PROD

El deploy a produccion se realiza automaticamente via GitHub Actions al crear un tag semantico (`v*`). Ver `.github/workflows/cd-prod.yml`.

## CI/CD

| Evento | Accion |
|---|---|
| Push a `feature/*` | Validate + deploy DEV |
| PR a `main` | Validate + deploy STG |
| Tag `v*` | Deploy PROD |

## Tests

```bash
uv run pytest
```
