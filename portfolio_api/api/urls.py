from django.urls import path

from .views import portfolio_advanced_metrics, portfolio_metrics

urlpatterns = [
    path("portfolios/<str:portfolio_id>/metrics", portfolio_metrics),
    path("portfolios/<str:portfolio_id>/advanced-metrics", portfolio_advanced_metrics),
]
