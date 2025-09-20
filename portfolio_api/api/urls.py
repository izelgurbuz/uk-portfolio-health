from django.urls import path

from .views import portfolio_metrics

urlpatterns = [
    path("portfolios/<str:portfolio_id>/metrics", portfolio_metrics),
]
