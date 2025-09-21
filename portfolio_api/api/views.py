import numpy as np
import pandas as pd
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .snowflake_client import get_snowflake_conn


@api_view(["GET"])
def portfolio_metrics(request, portfolio_id):
    """
    Return the last 30 days of portfolio metrics for the given portfolio.
    """
    query = f"""
        SELECT *
        FROM PORTFOLIO.ANALYTICS.VIEW_PORTFOLIO_METRICS
        WHERE PORTFOLIO_ID = '{portfolio_id}'
        ORDER BY DATE DESC
        LIMIT 30
    """

    # Connect to Snowflake
    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get column names dynamically
        columns = [desc[0] for desc in cursor.description]

        # Convert to Pandas DataFrame
        df = pd.DataFrame(rows, columns=columns)
        # Debug check
        print("BEFORE CLEANING:", df.to_dict(orient="records"))

        # Replace NaN (created by Pandas) with None for JSON serialization
        df = df.replace({np.nan: None})

        # Debug check after cleaning
        print("AFTER CLEANING:", df.to_dict(orient="records"))

    return Response(df.to_dict(orient="records"))


@api_view(["GET"])
def portfolio_advanced_metrics(request, portfolio_id):
    query = f"""
        SELECT *
        FROM FACT_PORTFOLIO_ADV_METRICS
        WHERE PORTFOLIO_ID = '{portfolio_id}'
        ORDER BY DATE DESC
        LIMIT 30
    """

    with get_snowflake_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        import numpy as np
        import pandas as pd

        df = pd.DataFrame(rows, columns=columns)

        # Replace NaN with None
        df = df.replace({np.nan: None})

    return Response(df.to_dict(orient="records"))


def dashboard(request, portfolio_id="P1"):
    query = f"""
        SELECT DATE, TOTAL_VALUE_GBP, WEIGHTED_DAILY_RETURN
        FROM VIEW_PORTFOLIO_METRICS
        WHERE PORTFOLIO_ID = '{portfolio_id}'
        ORDER BY DATE
        LIMIT 100
    """

    with get_snowflake_conn() as conn:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=cols)
    df = df.replace({np.nan: None})

    context = {
        "portfolio_id": portfolio_id,
        "dates": [str(d) for d in df["DATE"]],
        "values": [round(v, 2) if v else None for v in df["TOTAL_VALUE_GBP"]],
        "returns": [round(r, 4) if r else None for r in df["WEIGHTED_DAILY_RETURN"]],
    }

    return render(request, "dashboard.html", context)
