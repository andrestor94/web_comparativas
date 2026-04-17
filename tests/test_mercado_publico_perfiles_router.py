from pathlib import Path
import sys

from sqlalchemy import column, select, table

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_comparativas.routers.mercado_publico_perfiles_router import (
    _apply_filter_search_context,
    _apply_exact_text,
    _resolve_grouped_primary_value,
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


def test_apply_filter_search_context_excludes_current_field_but_keeps_other_filters():
    rows = table(
        "comparativa_rows",
        column("descripcion"),
        column("marca"),
        column("proveedor"),
        column("rubro"),
        column("plataforma"),
        column("fecha_apertura"),
    )
    stmt = _apply_filter_search_context(
        select(rows.c.marca),
        "marca",
        descripcion="Producto A",
        marca="Marca 1",
        proveedor="Proveedor 1",
        rubro="Rubro 1",
        plataforma="SIPROSA",
    )

    sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "descripcion IN ('Producto A')" in sql
    assert "proveedor IN ('Proveedor 1')" in sql
    assert "rubro IN ('Rubro 1')" in sql
    assert "plataforma IN ('SIPROSA')" in sql
    assert "marca IN ('Marca 1')" not in sql


def test_resolve_grouped_primary_value_prefers_highest_count_then_name():
    result = _resolve_grouped_primary_value([
        ("Traumatologia", 2),
        ("Anestesia", 5),
        ("Cardiologia", 5),
    ])

    assert result["value"] == "Anestesia"
    assert result["count"] == 3
    assert result["multiple"] is True
    assert result["values"] == ["Anestesia", "Cardiologia", "Traumatologia"]
