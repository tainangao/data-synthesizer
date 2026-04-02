"""
Detailed EDA for Credit Risk Dataset Submission.
Use # %% markers to run by notebook-style cells in VS Code.
"""

# %% Setup and Data Loading
import json
import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def resolve_project_root() -> Path:
    """Resolve project root for both script and notebook execution."""
    if "__file__" in globals():
        return Path(__file__).resolve().parents[1]

    cwd = Path.cwd()
    if cwd.name == "tests":
        return cwd.parent
    return cwd


ROOT_DIR = resolve_project_root()
OUTPUT_DIR = ROOT_DIR / "demo_output"
DB_PATH = OUTPUT_DIR / "sqlite" / "data.db"
SCHEMA_PATH = OUTPUT_DIR / "schema.json"

sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

if not DB_PATH.exists():
    raise FileNotFoundError(f"Database not found: {DB_PATH}")
if not SCHEMA_PATH.exists():
    raise FileNotFoundError(f"Schema not found: {SCHEMA_PATH}")

schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

with sqlite3.connect(DB_PATH) as conn:
    available_tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn
    )["name"].tolist()

    required_tables = ["Customers", "CreditApplications", "RiskAssessments"]
    missing_tables = [t for t in required_tables if t not in available_tables]
    if missing_tables:
        raise RuntimeError(
            f"Missing expected tables: {missing_tables}. Available: {available_tables}"
        )

    customers = pd.read_sql_query("SELECT * FROM Customers", conn)
    credit_applications = pd.read_sql_query("SELECT * FROM CreditApplications", conn)
    risk_assessments = pd.read_sql_query("SELECT * FROM RiskAssessments", conn)

print(f"Schema: {schema['schema_name']}")
print(f"Customers: {len(customers)} rows")
print(f"CreditApplications: {len(credit_applications)} rows")
print(f"RiskAssessments: {len(risk_assessments)} rows")

# Standardize datetime columns used in analysis
customers["date_of_birth"] = pd.to_datetime(customers["date_of_birth"], errors="coerce")
customers["registration_date"] = pd.to_datetime(
    customers["registration_date"], errors="coerce"
)
credit_applications["application_date"] = pd.to_datetime(
    credit_applications["application_date"], errors="coerce"
)
risk_assessments["assessment_date"] = pd.to_datetime(
    risk_assessments["assessment_date"], errors="coerce"
)


# %% Customer Demographics
customers["age"] = (
    pd.Timestamp.now().normalize() - customers["date_of_birth"]
).dt.days / 365.25

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

axes[0, 0].hist(customers["age"].dropna(), bins=25, edgecolor="black", alpha=0.75)
axes[0, 0].set_title("Customer Age Distribution")
axes[0, 0].set_xlabel("Age")
axes[0, 0].set_ylabel("Count")

gender_counts = customers["gender"].fillna("Missing").value_counts(dropna=False)
axes[0, 1].bar(gender_counts.index.astype(str), gender_counts.values, edgecolor="black")
axes[0, 1].set_title("Gender Distribution")
axes[0, 1].set_xlabel("Gender")
axes[0, 1].set_ylabel("Count")
axes[0, 1].tick_params(axis="x", rotation=30)

segment_counts = (
    customers["customer_segment"].fillna("Missing").value_counts(dropna=False)
)
axes[1, 0].bar(
    segment_counts.index.astype(str),
    segment_counts.values,
    edgecolor="black",
    color="teal",
)
axes[1, 0].set_title("Customer Segment Distribution")
axes[1, 0].set_xlabel("Segment")
axes[1, 0].set_ylabel("Count")
axes[1, 0].tick_params(axis="x", rotation=30)

has_email = customers["email_address"].notna().sum()
axes[1, 1].bar(
    ["With Email", "Without Email"],
    [has_email, len(customers) - has_email],
    edgecolor="black",
    color="orange",
)
axes[1, 1].set_title("Email Availability")
axes[1, 1].set_xlabel("Email Status")
axes[1, 1].set_ylabel("Count")

plt.tight_layout()
plt.show()

print("\n=== Customer Summary ===")
print(f"Age mean={customers['age'].mean():.1f}, std={customers['age'].std():.1f}")
print(f"Gender distribution:\n{customers['gender'].value_counts(dropna=False)}")
print(
    "Email availability: "
    f"{has_email}/{len(customers)} ({has_email / max(len(customers), 1) * 100:.1f}%)"
)


# %% Credit Application Analysis
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

loan_type_counts = (
    credit_applications["loan_type"].fillna("Missing").value_counts(dropna=False)
)
axes[0, 0].bar(
    loan_type_counts.index.astype(str), loan_type_counts.values, edgecolor="black"
)
axes[0, 0].set_title("Loan Type Distribution")
axes[0, 0].set_xlabel("Loan Type")
axes[0, 0].set_ylabel("Count")
axes[0, 0].tick_params(axis="x", rotation=30)

app_status_counts = (
    credit_applications["application_status"]
    .fillna("Missing")
    .value_counts(dropna=False)
)
axes[0, 1].bar(
    app_status_counts.index.astype(str),
    app_status_counts.values,
    edgecolor="black",
    color="firebrick",
)
axes[0, 1].set_title("Application Status Distribution")
axes[0, 1].set_xlabel("Status")
axes[0, 1].set_ylabel("Count")
axes[0, 1].tick_params(axis="x", rotation=30)

axes[1, 0].hist(
    credit_applications["requested_loan_amount"].dropna(),
    bins=30,
    edgecolor="black",
    alpha=0.75,
)
axes[1, 0].set_title("Requested Loan Amount Distribution")
axes[1, 0].set_xlabel("Requested Loan Amount")
axes[1, 0].set_ylabel("Count")

axes[1, 1].hist(
    np.log10(credit_applications["stated_income"].dropna() + 1),
    bins=30,
    edgecolor="black",
    alpha=0.75,
    color="forestgreen",
)
axes[1, 1].set_title("Stated Income Distribution (log10)")
axes[1, 1].set_xlabel("log10(Income)")
axes[1, 1].set_ylabel("Count")

plt.tight_layout()
plt.show()

print("\n=== Credit Application Summary ===")
print(f"Total applications: {len(credit_applications)}")
print(
    f"Application status:\n{credit_applications['application_status'].value_counts(dropna=False)}"
)
print(
    "Stated income: "
    f"mean=${credit_applications['stated_income'].mean():,.0f}, "
    f"median=${credit_applications['stated_income'].median():,.0f}"
)
print(
    "Requested amount: "
    f"mean=${credit_applications['requested_loan_amount'].mean():,.0f}, "
    f"median=${credit_applications['requested_loan_amount'].median():,.0f}"
)


# %% Risk Assessment Analysis
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

risk_grade_counts = (
    risk_assessments["risk_grade"].fillna("Missing").value_counts(dropna=False)
)
axes[0, 0].bar(
    risk_grade_counts.index.astype(str), risk_grade_counts.values, edgecolor="black"
)
axes[0, 0].set_title("Risk Grade Distribution")
axes[0, 0].set_xlabel("Risk Grade")
axes[0, 0].set_ylabel("Count")

axes[0, 1].hist(
    risk_assessments["risk_score"].dropna(), bins=30, edgecolor="black", alpha=0.75
)
axes[0, 1].set_title("Risk Score Distribution")
axes[0, 1].set_xlabel("Risk Score")
axes[0, 1].set_ylabel("Count")

axes[1, 0].hist(
    risk_assessments["approved_amount"].dropna(),
    bins=30,
    edgecolor="black",
    alpha=0.75,
    color="slateblue",
)
axes[1, 0].set_title("Approved Amount Distribution")
axes[1, 0].set_xlabel("Approved Amount")
axes[1, 0].set_ylabel("Count")

axes[1, 1].hist(
    risk_assessments["interest_rate_offered"].dropna(),
    bins=30,
    edgecolor="black",
    alpha=0.75,
    color="darkorange",
)
axes[1, 1].set_title("Interest Rate Offered Distribution")
axes[1, 1].set_xlabel("Interest Rate")
axes[1, 1].set_ylabel("Count")

plt.tight_layout()
plt.show()

print("\n=== Risk Assessment Summary ===")
print(f"Total assessments: {len(risk_assessments)}")
print(
    f"Risk grade distribution:\n{risk_assessments['risk_grade'].value_counts(dropna=False)}"
)
print(
    "Risk score mean="
    f"{risk_assessments['risk_score'].mean():.1f} "
    f"(min={risk_assessments['risk_score'].min():.1f}, "
    f"max={risk_assessments['risk_score'].max():.1f})"
)
print(
    "Approved amount: "
    f"mean=${risk_assessments['approved_amount'].mean():,.0f}, "
    f"median=${risk_assessments['approved_amount'].median():,.0f}"
)


# %% Relationship and Temporal Checks
apps_risk = credit_applications.merge(risk_assessments, on="application_id", how="left")
apps_risk_full = apps_risk.merge(
    customers, on="customer_id", how="left", suffixes=("", "_cust")
)

apps_assessed = risk_assessments["application_id"].nunique()
coverage = apps_assessed / max(len(credit_applications), 1) * 100

apps_risk["assessment_lag_days"] = (
    apps_risk["assessment_date"] - apps_risk["application_date"]
).dt.days
negative_lag = (apps_risk["assessment_lag_days"].dropna() < 0).sum()

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

axes[0, 0].bar(
    ["Assessed", "Not Assessed"],
    [apps_assessed, len(credit_applications) - apps_assessed],
    edgecolor="black",
    color=["seagreen", "firebrick"],
)
axes[0, 0].set_title("Assessment Coverage")
axes[0, 0].set_ylabel("Applications")

axes[0, 1].scatter(
    apps_risk["requested_loan_amount"],
    apps_risk["approved_amount"],
    alpha=0.55,
)
axes[0, 1].set_title("Requested vs Approved Amount")
axes[0, 1].set_xlabel("Requested Loan Amount")
axes[0, 1].set_ylabel("Approved Amount")

axes[1, 0].scatter(
    apps_risk_full["stated_income"],
    apps_risk_full["requested_loan_amount"],
    alpha=0.55,
    color="teal",
)
axes[1, 0].set_title("Stated Income vs Requested Amount")
axes[1, 0].set_xlabel("Stated Income")
axes[1, 0].set_ylabel("Requested Amount")

risk_by_grade = (
    apps_risk.assign(risk_grade=apps_risk["risk_grade"].fillna("Missing"))
    .groupby("risk_grade")["risk_score"]
    .mean()
    .sort_values(ascending=False)
)
axes[1, 1].bar(risk_by_grade.index.astype(str), risk_by_grade.values, edgecolor="black")
axes[1, 1].set_title("Average Risk Score by Risk Grade")
axes[1, 1].set_xlabel("Risk Grade")
axes[1, 1].set_ylabel("Average Risk Score")

plt.tight_layout()
plt.show()

print("\n=== Relationship and Temporal Checks ===")
print(
    f"Assessment coverage: {apps_assessed}/{len(credit_applications)} ({coverage:.1f}%)"
)
print(f"Rows with assessment_date before application_date: {negative_lag}")

orphan_apps = credit_applications[
    ~credit_applications["customer_id"].isin(customers["customer_id"])
]
orphan_assessments = risk_assessments[
    ~risk_assessments["application_id"].isin(credit_applications["application_id"])
]
print(f"Orphaned applications (missing customer): {len(orphan_apps)}")
print(f"Orphaned assessments (missing application): {len(orphan_assessments)}")


# %% Semi-Structured Data Validation
print("\n=== Semi-Structured Data Validation ===")

address_non_null = customers["address_details_json"].notna().sum()
print(
    "Customers.address_details_json non-null: "
    f"{address_non_null} ({address_non_null / max(len(customers), 1) * 100:.1f}%)"
)

valid_address_json = 0
observed_states = []
for raw in customers["address_details_json"].dropna():
    try:
        parsed = json.loads(raw)
        valid_address_json += 1
        state_val = parsed.get("state") or parsed.get("state_code")
        if isinstance(state_val, str) and state_val.strip():
            observed_states.append(state_val.strip())
    except (TypeError, json.JSONDecodeError):
        pass

print(f"Valid address JSON records: {valid_address_json}/{address_non_null}")
if observed_states:
    state_counts = pd.Series(observed_states).value_counts().head(10)
    print(f"Top observed state values:\n{state_counts}")

xml_non_null = risk_assessments["assessment_details_xml"].notna().sum()
valid_xml = 0
for raw in risk_assessments["assessment_details_xml"].dropna():
    try:
        ET.fromstring(raw)
        valid_xml += 1
    except ET.ParseError:
        pass

print(f"RiskAssessments.assessment_details_xml valid XML: {valid_xml}/{xml_non_null}")


# %% Financial Scale Sanity Checks
print("\n=== Financial Scale Sanity Checks ===")

financial_cols = {
    "CreditApplications.stated_income": credit_applications["stated_income"],
    "CreditApplications.requested_loan_amount": credit_applications[
        "requested_loan_amount"
    ],
    "RiskAssessments.approved_amount": risk_assessments["approved_amount"],
}

for name, series in financial_cols.items():
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        print(f"{name}: no numeric values")
        continue

    print(
        f"{name}: mean=${cleaned.mean():,.0f}, median=${cleaned.median():,.0f}, "
        f"p95=${cleaned.quantile(0.95):,.0f}"
    )

interest = pd.to_numeric(
    risk_assessments["interest_rate_offered"], errors="coerce"
).dropna()
if not interest.empty:
    print(
        "RiskAssessments.interest_rate_offered: "
        f"mean={interest.mean():.2f}, min={interest.min():.2f}, max={interest.max():.2f}"
    )


# %% Final Summary
print("\n" + "=" * 80)
print("FINAL SUBMISSION SUMMARY")
print("=" * 80)
print(f"Customers: {len(customers):,}")
print(f"CreditApplications: {len(credit_applications):,}")
print(f"RiskAssessments: {len(risk_assessments):,}")
print(f"Assessment coverage: {coverage:.1f}%")
print(f"Temporal violations (assessment before application): {negative_lag}")
print(f"Address JSON valid rate: {valid_address_json}/{address_non_null}")
print(f"Assessment XML valid rate: {valid_xml}/{xml_non_null}")
print("EDA complete.")
print("=" * 80)
