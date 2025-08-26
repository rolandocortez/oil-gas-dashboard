# Forecasting (H6.S2)

This folder contains the modeling pipeline to compare **Naive**, **ARIMA** (optional) and **Prophet** (optional) on the annual oil series.

## Inputs
- `docs/eda/oil_yearly_series.csv` (from H6.S1) — required.
  - Must include a `year` column and one numeric value column (e.g., `oil_bbl`).

## Outputs (created in `docs/forecast/`)
- `metrics.csv` — MAE / RMSE per model
- `predictions.csv` — predictions on the temporal test window
- `model_choice.json` — best model by RMSE
- `future_forecast.csv` — 3-year forward forecast using the selected model

## Use the notebook
Open: `notebooks/production_forecast_modeling.ipynb` and run all cells.

## Use the CLI
```bash
python scripts/run_forecast.py \
  --input docs/eda/oil_yearly_series.csv \
  --value-col oil_bbl \
  --outdir docs/forecast \
  --test-years 2 \
  --future-years 3
```
ARIMA requires `statsmodels`; Prophet requires `prophet`. If they are not installed, the pipeline runs with Naive only.

## Notes
- Split is done **temporally** (last N years as test).
- Metrics: **MAE**, **RMSE**.
- The selected model is persisted in `model_choice.json`.
- Designed to be simple and robust for portfolio use.
