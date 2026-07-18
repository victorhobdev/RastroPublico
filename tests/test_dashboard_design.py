import json
import runpy


dashboard_module = runpy.run_path("scripts/redesign_dashboard.py", run_name="dashboard")
build_dashboard = dashboard_module["build_dashboard"]
validate_dashboard = dashboard_module["validate_dashboard"]


def test_dashboard_uses_only_audited_portfolio_metrics() -> None:
    dashboard = build_dashboard()
    validate_dashboard(dashboard)
    serialized = json.dumps(dashboard, ensure_ascii=False)

    for expected in ("12.252", "29.207", "4.246", "1.537", "828", "1.755"):
        assert expected in serialized
    for invalidated in ("317.043", "2,63 milhões", "106.494", "52.767", "11.548"):
        assert invalidated not in serialized
    assert dashboard["datasets"] == []


def test_dashboard_does_not_publish_monetary_rankings() -> None:
    serialized = json.dumps(build_dashboard(), ensure_ascii=False).lower()

    assert "ranking de preços" in serialized
    assert "suprimidos" in serialized
    assert "valor homologado" not in serialized
