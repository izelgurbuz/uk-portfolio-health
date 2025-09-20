import numpy as np
import pandas as pd
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
