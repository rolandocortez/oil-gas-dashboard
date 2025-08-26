#!/usr/bin/env python3
import argparse
import os
import json
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


def mae(y_true, y_pred):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true, y_pred):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def detect_columns(df, value_col=None):
    year_col = None
    for c in df.columns:
        if c.lower().strip() in ("year", "reporting_year"):
            year_col = c
            break
    if year_col is None:
        raise ValueError("No 'year' or 'reporting_year' column found.")
    if value_col is None:
        for cand in ["oil_bbl", "oil", "value", "y"]:
            if cand in df.columns:
                value_col = cand
                break
    if value_col is None:
        num_cols = [
            c
            for c in df.columns
            if c != year_col and pd.api.types.is_numeric_dtype(df[c])
        ]
        if not num_cols:
            raise ValueError("No numeric value column found.")
        value_col = num_cols[0]
    return year_col, value_col


def fit_predict(train, test):
    results = []
    preds_rows = []

    # Naive
    last_value = float(train["y"].iloc[-1])
    naive_pred = np.full(len(test), last_value, dtype=float)
    results.append(
        {
            "model": "naive_last",
            "mae": mae(test["y"], naive_pred),
            "rmse": rmse(test["y"], naive_pred),
        }
    )
    preds_rows += [
        {"model": "naive_last", "ds": ds, "y_true": yt, "y_pred": yp}
        for ds, yt, yp in zip(test["ds"], test["y"], naive_pred)
    ]

    # ARIMA (optional)
    try:
        import itertools
        from statsmodels.tsa.arima.model import ARIMA

        best = None
        for order in itertools.product([0, 1, 2], [0, 1, 2], [0, 1, 2]):
            try:
                mdl = ARIMA(train["y"], order=order).fit()
                fc = mdl.forecast(steps=len(test))
                cand = (rmse(test["y"], fc), order, fc)
                if best is None or cand < best:
                    best = cand
            except Exception:
                continue
        if best:
            b_rmse, order, fc = best
            results.append(
                {"model": f"arima{order}", "mae": mae(test["y"], fc), "rmse": b_rmse}
            )
            preds_rows += [
                {"model": f"arima{order}", "ds": ds, "y_true": yt, "y_pred": yp}
                for ds, yt, yp in zip(test["ds"], test["y"], fc)
            ]
    except Exception as e:
        print("[warn] ARIMA not available or failed:", e)

    # Prophet (optional)
    try:
        from prophet import Prophet

        pf = Prophet(
            growth="linear",
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
        pf.fit(train.rename(columns={"ds": "ds", "y": "y"}))
        fc = pf.predict(pd.DataFrame({"ds": test["ds"]}))["yhat"].values
        results.append(
            {"model": "prophet", "mae": mae(test["y"], fc), "rmse": rmse(test["y"], fc)}
        )
        preds_rows += [
            {"model": "prophet", "ds": ds, "y_true": yt, "y_pred": yp}
            for ds, yt, yp in zip(test["ds"], test["y"], fc)
        ]
    except Exception as e:
        print("[warn] Prophet not available or failed:", e)

    return pd.DataFrame(results), pd.DataFrame(preds_rows)


def forecast_future(d_all, selected_model, years=3):
    last_ds = d_all["ds"].max()
    future_dates = pd.date_range(
        last_ds + pd.offsets.DateOffset(years=1), periods=years, freq="YS"
    )
    if selected_model.startswith("naive"):
        vals = np.full(len(future_dates), float(d_all["y"].iloc[-1]))
    elif selected_model.startswith("arima"):
        from statsmodels.tsa.arima.model import ARIMA

        order = eval(selected_model.replace("arima", ""))
        mdl = ARIMA(d_all["y"], order=order).fit()
        vals = mdl.forecast(steps=len(future_dates)).values
    elif selected_model == "prophet":
        from prophet import Prophet

        pf = Prophet(
            growth="linear",
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
        pf.fit(d_all.rename(columns={"ds": "ds", "y": "y"}))
        vals = pf.predict(pd.DataFrame({"ds": future_dates}))["yhat"].values
    else:
        vals = np.full(len(future_dates), float(d_all["y"].iloc[-1]))
    return pd.DataFrame({"ds": future_dates, "yhat": vals})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        required=True,
        help="Path to CSV (with 'year' col and a numeric value col).",
    )
    ap.add_argument(
        "--value-col",
        default=None,
        help="Value column name (auto-detected if omitted).",
    )
    ap.add_argument("--outdir", default="docs/forecast", help="Output directory.")
    ap.add_argument("--test-years", type=int, default=2)
    ap.add_argument("--future-years", type=int, default=3)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_csv(args.input)
    year_col, value_col = detect_columns(df, args.value_col)

    d = df[[year_col, value_col]].dropna().copy()
    d = d.sort_values(year_col).reset_index(drop=True)
    d["ds"] = pd.to_datetime(d[year_col].astype(int).astype(str) + "-01-01")
    d = d.rename(columns={value_col: "y"})
    d = d[["ds", "y"]]

    cutoff = len(d) - args.test_years
    if cutoff < 5:
        raise ValueError(
            "Not enough data for the requested test window. Reduce --test-years."
        )
    train = d.iloc[:cutoff].reset_index(drop=True)
    test = d.iloc[cutoff:].reset_index(drop=True)

    metrics, preds = fit_predict(train, test)
    metrics = metrics.sort_values("rmse").reset_index(drop=True)

    metrics_path = os.path.join(args.outdir, "metrics.csv")
    preds_path = os.path.join(args.outdir, "predictions.csv")
    metrics.to_csv(metrics_path, index=False)
    preds.to_csv(preds_path, index=False)

    best = metrics.iloc[0]["model"]
    with open(os.path.join(args.outdir, "model_choice.json"), "w") as f:
        json.dump({"selected_model": best}, f, indent=2)

    fut = forecast_future(d, best, years=args.future_years)
    fut.to_csv(os.path.join(args.outdir, "future_forecast.csv"), index=False)

    print("Saved:", metrics_path, preds_path, "and future_forecast.csv")
    print("Best model:", best)
    print(metrics)


if __name__ == "__main__":
    main()
