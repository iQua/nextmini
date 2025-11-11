from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax

console = Console()


def panel(title: str, body: str) -> None:
    console.print(Panel(body, title=title))


def show_toml(toml_str: str, title: str = "Config") -> None:
    console.print(Syntax(toml_str, "toml", theme="monokai", word_wrap=True), markup=False)


def metrics_table(metrics: dict[str, str | int | float]) -> None:
    table = Table(title="Session Metrics")
    table.add_column("Metric")
    table.add_column("Value")
    for k, v in metrics.items():
        table.add_row(str(k), str(v))
    console.print(table)

