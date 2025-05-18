# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: sandbox
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Exploratory Data Analysis
#
# This notebook is for doing data checks and quick visualizations.

# %%
# standard import
# warnings
import warnings

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# advanced analytics
import statsmodels.api as sm
from statsmodels.tsa.arima.model import ARIMA

warnings.filterwarnings("ignore")

# establish database connections
conn = duckdb.connect("../data/lake/database.duckdb")

# %% [markdown]
# ## Overview
#
# Load and merge tables.

# %%
# climate_atmosphere data
climate_df = conn.sql(
    """
    SELECT
        date,
        adm3_en,
        adm3_pcode,
        adm4_en,
        adm4_pcode,
        tave,
        pr
    FROM public.project_cchain_climate_atmosphere_clean
    WHERE date >= '2020-01-01' AND date < '2022-12-31'
    ORDER BY date
    """
).fetchdf()

climate_df = climate_df.drop_duplicates().reset_index()
climate_df["date"] = pd.to_datetime(climate_df["date"])
climate_df = climate_df.set_index("date")

daily_avg_df = (
    climate_df.groupby("adm3_pcode")
    .resample("D")
    .agg(
        {
            "adm3_en": "first",
            "adm4_pcode": "first",
            "tave": "mean",
            "pr": "mean",  # average per day
        }
    )
    .reset_index()
)

climate_df = (
    daily_avg_df.groupby("adm3_pcode")
    .resample("W-Mon", on="date")
    .agg(
        {
            "adm3_en": "first",
            "tave": "mean",
            "pr": "sum",  # sum per week
        }
    )
    .reset_index()
)

climate_df = climate_df[climate_df["date"] != "2023-01-02"]

climate_df.head(5)

# %%
# dengue data
dengue_df = conn.sql(
    """
    SELECT
        date,
        adm3_en,
        adm3_pcode,
        disease_common_name,
        case_total
    FROM public.project_cchain_disease_pidsr_totals_clean
    WHERE date >= '2020-01-01' AND date < '2022-12-31'
    """
).fetchdf()

dengue_df = dengue_df.drop_duplicates().reset_index(drop=True)
dengue_df["date"] = pd.to_datetime(dengue_df["date"])
dengue_df = dengue_df.pivot(
    index=["date", "adm3_en", "adm3_pcode"],
    columns="disease_common_name",
    values="case_total",
).reset_index()

dengue_df = dengue_df.rename(
    columns={
        "ACUTE BLOODY DIARRHEA": "diarrhea_cases",
        "CHIKUNGUYA VIRAL DISEASE": "chikungunya_cases",
        "CHOLERA": "cholera_cases",
        "DENGUE FEVER": "dengue_cases",
        "HEPATITIS A": "hepa_a_cases",
        "LEPTOSPIROSIS": "lepto_cases",
        "RABIES": "rabies_cases",
        "ROTAVIRAL ENTERITIS": "rotavirus_cases",
        "TYPHOID FEVER": "typhoid_cases",
    }
)

dengue_df.head(5)

# %%
# merged_df
merged_df = pd.merge(
    climate_df, dengue_df, on=["date", "adm3_en", "adm3_pcode"], how="left"
)
merged_df.set_index("date", inplace=True)
merged_df.head(5)

# %% [markdown]
# ## Descriptive statistics

# %%
# descriptive stats
describe_df = merged_df.groupby(["adm3_pcode", "adm3_en"]).describe().reset_index()
describe_df.head(5)

# %%
# mean by city
summary_df = merged_df.groupby(["adm3_pcode", "adm3_en"]).mean().reset_index()
summary_df.head(5)

# %% [markdown]
# ## Regression
#
# For this section onwards, we are showing visualizations for **precipitation**, **average temperature**, and **dengue**.

# %%
# disease_list
disease_names = [
    # "diarrhea_cases",
    # "chikungunya_cases",
    # "cholera_cases",
    "dengue_cases",
    # "hepa_a_cases",
    # "lepto_cases",
    # "rabies_cases",
    # "rotavirus_cases",
    # "typhoid_cases",
]

# city list
# adm3_en_list = merged_df["adm3_en"].unique()
adm3_en_list = [
    "Dagupan City",
    # "Palayan City",
    # "Legazpi City",
    # "Iloilo City",
    # "Mandaue City",
    # "Tacloban City",
    # "Zamboanga City",
    # "Cagayan de Oro City",
    # "Davao City",
    "City of Mandaluyong",
    # "City of Navotas",
    # "City of Muntinlupa"
]

# climate variables list
climate_variables = ["tave", "pr"]

# %%
# prep data for regression
X = merged_df[climate_variables]
y = merged_df[disease_names]
X = sm.add_constant(X)

# fit regression model
model = sm.OLS(y, X).fit()
print(model.summary())

# %% [markdown]
# ## Visualizations

# %% [markdown]
# ### Time series (seasonal decomposition)

# %%
# seasonal decomposition details
seasonal_decompose_model = "additive"  # additive | multiplicative
component = "seasonal"  # observed | trend | seasonal | resid

for adm3 in adm3_en_list:
    adm3_data = merged_df[merged_df["adm3_en"] == adm3]

    plt.figure(figsize=(12, 4))
    ax1 = plt.gca()
    ax2 = ax1.twinx()

    for disease in disease_names:
        if disease in adm3_data.columns:
            disease_decomposition = sm.tsa.seasonal_decompose(
                adm3_data[disease], model=seasonal_decompose_model
            )
            disease_component = getattr(disease_decomposition, component)
            ax1.plot(
                disease_component.index,
                disease_component,
                label=disease.replace("_cases", ""),
                linewidth=1,
            )

    for climate_var in climate_variables:
        if climate_var in adm3_data.columns:
            climate_var_decomposition = sm.tsa.seasonal_decompose(
                adm3_data[climate_var], model=seasonal_decompose_model
            )
            climate_var_component = getattr(climate_var_decomposition, component)
            ax2.plot(
                climate_var_component.index,
                climate_var_component,
                label=climate_var,
                linewidth=0.5,
                linestyle="--",
            )

    ax1.set_title(f"{adm3}, seasonal decomposition ({component})")
    ax1.set_ylabel("cases")
    ax2.set_ylabel("precipitation (mm)")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.show()

# %% [markdown]
# ### Correlation

# %%
correlation_matrices = []

for adm3 in adm3_en_list:
    adm3_data = merged_df[merged_df["adm3_en"] == adm3]

    correlation_matrix = adm3_data[disease_names + climate_variables].corr()
    correlation_matrix["adm3_en"] = adm3
    correlation_matrices.append(correlation_matrix)

correlation_df = pd.concat(correlation_matrices)
correlation_df = correlation_df.pivot(
    index=["adm3_en"], columns=[], values=disease_names + climate_variables
).reset_index()
# correlation_df.reset_index(inplace=True)
correlation_df.head(10)

# %%
for adm3 in adm3_en_list:
    adm3_correlation_data = correlation_df[correlation_df["adm3_en"] == adm3]
    adm3_correlation_matrix = adm3_correlation_data.drop(columns=["adm3_en"])
    square_correlation_matrix = adm3_correlation_matrix.corr()

    plt.figure(figsize=(4, 3))
    sns.heatmap(square_correlation_matrix, annot=True, cmap="viridis", cbar=True)
    plt.title(f"{adm3} Correlation Heatmap", fontsize=8)
    plt.xticks(rotation=0, fontsize=8)
    plt.yticks(fontsize=8)
    plt.show()

# %% [markdown]
# ### Scatter Plot

# %%
for adm3 in adm3_en_list:
    adm3_data = merged_df[merged_df["adm3_en"] == adm3]

    plt.figure(figsize=(4, 3))
    plt.scatter(adm3_data["pr"], adm3_data["dengue_cases"])

    m, b = np.polyfit(adm3_data["pr"], adm3_data["dengue_cases"], 1)  # 1 for linear fit
    plt.plot(adm3_data["pr"], m * adm3_data["pr"] + b, color="red", label="Trendline")
    equation = f"y = {m:.2f}x + {b:.2f}"

    plt.title(f"{adm3}, precipitation vs. dengue", fontsize=8)
    plt.xlabel("Precipitation (mm)", fontsize=8)
    plt.ylabel("Dengue Cases", fontsize=8)
    plt.xticks(fontsize=6)
    plt.yticks(fontsize=6)
    plt.text(
        0.60,
        0.40,
        equation,
        transform=plt.gca().transAxes,
        fontsize=6,
        color="red",
        verticalalignment="top",
    )
    plt.show()

# %% [markdown]
# ### Forecasting (ARIMA)

# %%
forecast_steps = 100

for adm3 in adm3_en_list:
    adm3_data = merged_df[merged_df["adm3_en"] == adm3].reset_index()

    adm3_data["date"] = pd.to_datetime(adm3_data["date"], errors="coerce")
    adm3_data = adm3_data.set_index("date")
    adm3_data = adm3_data.sort_index()

    dengue_series = adm3_data["dengue_cases"]

    # fit arima model
    model = ARIMA(dengue_series, order=(1, 2, 1))  # to-do: check arima order
    model_fit = model.fit()

    print(f"ARIMA Model Summary for {adm3}:")
    print(model_fit.summary())

    forecast = model_fit.forecast(steps=forecast_steps)
    forecast_index = pd.date_range(
        start=dengue_series.index[-1] + pd.Timedelta(days=1),
        periods=forecast_steps,
        freq="D",
    )

    plt.figure(figsize=(12, 4))
    plt.plot(
        dengue_series.index, dengue_series, label="Dengue Cases", color="blue"
    )  # historical
    plt.plot(
        forecast_index, forecast, color="green", label="Forecast", linestyle="--"
    )  # forecast

    plt.title(f"{adm3}, Dengue Cases Over Time", fontsize=8)
    plt.xlabel("Date", fontsize=8)
    plt.ylabel("Dengue Cases", fontsize=8)
    plt.xticks(fontsize=6)
    plt.yticks(fontsize=6)
    plt.legend()
    plt.show()

# %%
