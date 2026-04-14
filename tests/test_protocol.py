from ndea.protocol import ChartPayload, TablePayload, TextPayload


def test_chart_payload_uses_echarts_renderer() -> None:
    payload = ChartPayload(
        title="Demo",
        option={"xAxis": {}, "yAxis": {}, "series": []},
        source=[],
    )
    assert payload.renderer == "echarts"


def test_table_payload_keeps_columns_and_rows() -> None:
    payload = TablePayload(columns=["name"], rows=[{"name": "Alice"}])
    assert payload.columns == ["name"]
    assert payload.rows[0]["name"] == "Alice"


def test_text_payload_keeps_summary() -> None:
    payload = TextPayload(summary="done")
    assert payload.summary == "done"
