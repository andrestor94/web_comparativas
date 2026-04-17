from pathlib import Path
import sys

from sqlalchemy import column, select, table

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_comparativas.routers.mercado_publico_perfiles_router import (
    _apply_exact_text,
    _split_filter_values,
)


def test_split_filter_values_trim_and_deduplicate():
    assert _split_filter_values("  Uno, Dos ,Uno,, Tres  ") == ["Uno", "Dos", "Tres"]


def test_apply_exact_text_uses_in_clause_for_selected_values():
    rows = table("comparativa_rows", column("descripcion"))
    stmt = _apply_exact_text(
        select(rows.c.descripcion),
        rows.c.descripcion,
        "Producto A,Producto B",
    )

    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "descripcion IN ('Producto A', 'Producto B')" in sql
