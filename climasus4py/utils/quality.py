"""Comprehensive data quality profiling for SUS datasets.

Mirrors R: climasus4r::sus_data_quality_report()

Sections: pipeline audit, overview, missing values, demographics,
dates, ICD-10 codes, geographic coverage, derived variables, quality score.

Output formats: console (rich), markdown, html.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# i18n labels
# ---------------------------------------------------------------------------

_QR_LABELS: dict[str, dict[str, str]] = {
    "report_title":    {"pt": "Relatório de Qualidade de Dados",    "en": "Data Quality Report",         "es": "Informe de Calidad de Datos"},
    "generated_at":    {"pt": "Gerado em: %s",                      "en": "Generated: %s",               "es": "Generado: %s"},
    "computing":       {"pt": "Calculando métricas de qualidade...", "en": "Computing quality metrics...", "es": "Calculando métricas de calidad..."},
    "saved_to":        {"pt": "Relatório salvo em: %s",             "en": "Report saved to: %s",         "es": "Informe guardado en: %s"},
    "quality_score":   {"pt": "Pontuação de Qualidade",             "en": "Quality Score",               "es": "Puntuación de Calidad"},
    "sec_pipeline":    {"pt": "0. Auditoria do Pipeline",           "en": "0. Pipeline Audit",           "es": "0. Auditoría del Pipeline"},
    "current_stage":   {"pt": "Etapa atual",                        "en": "Current stage",               "es": "Etapa actual"},
    "system_label":    {"pt": "Sistema",                            "en": "System",                      "es": "Sistema"},
    "type_label":      {"pt": "Tipo",                               "en": "Type",                        "es": "Tipo"},
    "fns_applied":     {"pt": "Funções aplicadas ao dataset",       "en": "Functions applied to dataset","es": "Funciones aplicadas al dataset"},
    "no_history":      {"pt": "Histórico de processamento vazio",   "en": "Processing history empty",    "es": "Historial de procesamiento vacío"},
    "stages_pending":  {"pt": "Etapas ainda não aplicadas",         "en": "Stages not yet applied",      "es": "Etapas aún no aplicadas"},
    "history_entries": {"pt": "Entradas no histórico",              "en": "History entries",             "es": "Entradas en el historial"},
    "history_log":     {"pt": "Log de Processamento",               "en": "Processing Log",              "es": "Registro de Procesamiento"},
    "more_entries":    {"pt": "entradas adicionais",                "en": "more entries",                "es": "entradas adicionales"},
    "sec_overview":    {"pt": "1. Visão Geral",                     "en": "1. Overview",                 "es": "1. Vista General"},
    "ov_rows":         {"pt": "Linhas",                             "en": "Rows",                        "es": "Filas"},
    "ov_cols":         {"pt": "Colunas",                            "en": "Columns",                     "es": "Columnas"},
    "ov_duplicates":   {"pt": "Duplicatas",                         "en": "Duplicates",                  "es": "Duplicados"},
    "sec_missing":     {"pt": "2. Completude (Valores Ausentes)",   "en": "2. Completeness (Missing Values)", "es": "2. Completitud (Valores Faltantes)"},
    "completeness_score": {"pt": "Pontuação de Completude",        "en": "Completeness Score",          "es": "Puntuación de Completitud"},
    "miss_complete_cols": {"pt": "Colunas completas",              "en": "Complete columns",            "es": "Columnas completas"},
    "miss_warn":       {"pt": "Colunas com aviso (5-20%)",          "en": "Warning columns (5-20%)",     "es": "Columnas con aviso (5-20%)"},
    "miss_critical":   {"pt": "Colunas críticas (>20%)",            "en": "Critical columns (>20%)",     "es": "Columnas críticas (>20%)"},
    "no_missing":      {"pt": "Nenhum valor ausente detectado",     "en": "No missing values detected",  "es": "No se detectaron valores faltantes"},
    "more_cols":       {"pt": "colunas adicionais",                 "en": "more columns",                "es": "columnas adicionales"},
    "sec_demographics":{"pt": "3. Variáveis Demográficas",          "en": "3. Demographic Variables",    "es": "3. Variables Demográficas"},
    "no_demo_cols":    {"pt": "Nenhuma coluna demográfica detectada","en": "No demographic columns detected","es": "No se detectaron columnas demográficas"},
    "sex":             {"pt": "Sexo",                               "en": "Sex",                         "es": "Sexo"},
    "race":            {"pt": "Raça/Cor",                           "en": "Race/Colour",                 "es": "Raza/Color"},
    "age":             {"pt": "Idade",                              "en": "Age",                         "es": "Edad"},
    "age_range":       {"pt": "Intervalo",                          "en": "Range",                       "es": "Rango"},
    "mean":            {"pt": "Média",                              "en": "Mean",                        "es": "Media"},
    "median":          {"pt": "Mediana",                            "en": "Median",                      "es": "Mediana"},
    "implausible":     {"pt": "Implausível (<0 ou >130)",           "en": "Implausible (<0 or >130)",    "es": "Implausible (<0 o >130)"},
    "education":       {"pt": "Escolaridade",                       "en": "Education",                   "es": "Escolaridad"},
    "climate_risk":    {"pt": "Risco Climático",                    "en": "Climate Risk",                "es": "Riesgo Climático"},
    "missing":         {"pt": "Ausente",                            "en": "Missing",                     "es": "Faltante"},
    "sec_dates":       {"pt": "4. Validação de Datas",              "en": "4. Date Validation",          "es": "4. Validación de Fechas"},
    "date_range":      {"pt": "Intervalo",                          "en": "Range",                       "es": "Rango"},
    "future":          {"pt": "Datas futuras",                      "en": "Future dates",                "es": "Fechas futuras"},
    "pre1900":         {"pt": "Antes de 1900",                      "en": "Before 1900",                 "es": "Antes de 1900"},
    "sec_icd":         {"pt": "5. Qualidade dos Códigos CID-10",    "en": "5. ICD-10 Code Quality",      "es": "5. Calidad de Códigos CIE-10"},
    "icd_unique":      {"pt": "Códigos únicos",                     "en": "Unique codes",                "es": "Códigos únicos"},
    "icd_valid":       {"pt": "Códigos válidos",                    "en": "Valid codes",                 "es": "Códigos válidos"},
    "icd_top":         {"pt": "Códigos mais frequentes",            "en": "Top ICD codes",               "es": "Códigos más frecuentes"},
    "icd_chapters":    {"pt": "Distribuição por capítulo",          "en": "Chapter distribution",        "es": "Distribución por capítulo"},
    "sec_geographic":  {"pt": "6. Cobertura Geográfica",            "en": "6. Geographic Coverage",      "es": "6. Cobertura Geográfica"},
    "municipalities":  {"pt": "Municípios",                         "en": "Municipalities",              "es": "Municipios"},
    "states":          {"pt": "Estados (UF)",                       "en": "States (UF)",                 "es": "Estados (UF)"},
    "sec_derived":     {"pt": "7. Variáveis Derivadas",             "en": "7. Derived Variables",        "es": "7. Variables Derivadas"},
    "variable":        {"pt": "Variável",                           "en": "Variable",                    "es": "Variable"},
    "present":         {"pt": "Presente",                           "en": "Present",                     "es": "Presente"},
    "absent":          {"pt": "Ausente",                            "en": "Absent",                      "es": "Ausente"},
    "metric":          {"pt": "Métrica",                            "en": "Metric",                      "es": "Métrica"},
    "value":           {"pt": "Valor",                              "en": "Value",                       "es": "Valor"},
    "status":          {"pt": "Status",                             "en": "Status",                      "es": "Estado"},
    "column":          {"pt": "Coluna",                             "en": "Column",                      "es": "Columna"},
    "source_note":     {"pt": "Dados de saúde brasileiros (DATASUS)","en": "Brazilian health data (DATASUS)","es": "Datos de salud brasileños (DATASUS)"},
}

_STAGE_ORDER = ["import", "clean", "stand", "filter_cid", "filter_demo",
                "derive", "aggregate", "spatial", "census", "climate"]

_STAGE_FN_MAP = {
    "import":      "sus_data_import()",
    "clean":       "sus_data_clean_encoding()",
    "stand":       "sus_data_standardize()",
    "filter_cid":  "sus_data_filter_cid()",
    "filter_demo": "sus_data_filter_demographics()",
    "derive":      "sus_data_create_variables()",
    "aggregate":   "sus_data_aggregate()",
    "spatial":     "sus_join_spatial()",
    "census":      "sus_socio_add_census()",
    "climate":     "sus_climate_*()",
}

_HISTORY_FN_PATTERNS = {
    "sus_data_import()":              r"Imported datasus|Imported DATASUS",
    "sus_data_read()":                r"Read .*files|Collected Arrow Dataset",
    "sus_data_clean_encoding()":      r"Cleaned character encoding",
    "sus_data_standardize()":         r"Standardized column names and types",
    "sus_data_filter_cid()":          r"Filtered by disease group|CID|ICD",
    "sus_data_filter_demographics()": r"Demographic filters|Filtered demographics|City:",
    "sus_data_create_variables()":    r"Derived variables created|Create variables|Created derived",
    "sus_data_aggregate()":           r"Temporal Data aggregated|Data aggregated|Aggregated by",
    "sus_join_spatial()":             r"Spatial Data aggregated|Spatial join",
    "sus_socio_add_census()":         r"Added census data",
    "sus_climate_aggregate()":        r"Climate aggregation",
    "sus_climate_inmet()":            r"INMET data imported",
}

_FLAG_TXT = {"ok": "[OK]", "warn": "[WARN]", "critical": "[CRIT]", "info": "[INFO]"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _qrl(key: str, lang: str, *args: Any) -> str:
    row = _QR_LABELS.get(key)
    if row is None:
        return key
    txt = row.get(lang) or row.get("pt") or key
    return (txt % args) if args else txt


def _qr_col(df: pd.DataFrame, patterns: list[str]) -> str | None:
    for p in patterns:
        if p in df.columns:
            return p
    return None


def _freq_tbl(df: pd.DataFrame, col: str, *, always_na: bool = True) -> list[dict]:
    n = len(df)
    vc = df[col].value_counts(dropna=False)
    out: list[dict] = []
    has_na = False
    for cat, cnt in vc.items():
        is_na = pd.isna(cat)
        has_na = has_na or is_na
        out.append({"category": None if is_na else cat,
                    "count": int(cnt),
                    "pct": round(100 * int(cnt) / max(n, 1), 1)})
    if always_na and not has_na:
        out.append({"category": None, "count": 0, "pct": 0.0})
    return out


def _as_dataframe(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    if hasattr(data, "df"):
        return data.df()
    if hasattr(data, "to_pandas"):
        return data.to_pandas()
    raise TypeError(
        "sus_data_quality_report: 'data' must be a DataFrame or DuckDBPyRelation "
        f"(got {type(data).__name__!r})."
    )


def _extract_meta(data: Any) -> dict:
    stage = system = type_ = "unknown"
    backend = "pandas"
    history: list[str] = []
    try:
        from ..core.engine import is_relation
        if is_relation(data):
            from ..core.meta import sus_meta
            stage   = sus_meta(data, "stage")   or "unknown"
            system  = sus_meta(data, "system")  or "unknown"
            type_   = sus_meta(data, "type")    or "unknown"
            history = sus_meta(data, "history") or []
            backend = "duckdb"
    except Exception:
        pass
    return {"stage": stage, "system": system, "type": type_,
            "backend": backend, "history": history}


def _is_datelike(s: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(s):
        return True
    if s.dtype == object:
        nn = s.dropna()
        if len(nn) == 0:
            return False
        return all(isinstance(v, (date, datetime)) for v in nn.head(200))
    return False


def _qr_detect_history_functions(entry: str) -> list[str]:
    msg = re.sub(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]\s*", "", entry)
    return [fn for fn, pattern in _HISTORY_FN_PATTERNS.items()
            if re.search(pattern, msg, re.IGNORECASE)]


# ---------------------------------------------------------------------------
# Section computations
# ---------------------------------------------------------------------------

def _qr_parse_history(history: list[str], current_stage: str) -> dict:
    try:
        stage_idx = _STAGE_ORDER.index(current_stage) + 1
    except ValueError:
        stage_idx = 0
    stages_reached  = _STAGE_ORDER[:stage_idx] if stage_idx > 0 else []
    fns_from_stages = [_STAGE_FN_MAP[s] for s in stages_reached if s in _STAGE_FN_MAP]
    fns_from_history: list[str] = []
    for entry in history:
        fns_from_history.extend(_qr_detect_history_functions(entry))
    functions_applied = list(dict.fromkeys(fns_from_stages + fns_from_history))
    pat = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*(.*)$")
    parsed = []
    for entry in history:
        m = pat.match(entry)
        parsed.append({"timestamp": m.group(1), "message": m.group(2)} if m
                      else {"timestamp": "", "message": entry})
    return {"stages_reached": stages_reached, "current_stage": current_stage,
            "functions_applied": functions_applied,
            "n_history_entries": len(history), "parsed_steps": parsed}


def _qr_overview(df: pd.DataFrame) -> dict:
    col_types: dict[str, int] = {}
    for dt in df.dtypes:
        key = str(dt)
        col_types[key] = col_types.get(key, 0) + 1
    n_dup  = int(df.duplicated().sum())
    n_rows = len(df)
    return {"n_rows": n_rows, "n_cols": df.shape[1], "n_dup": n_dup,
            "pct_dup": round(100 * n_dup / max(n_rows, 1), 2), "col_types": col_types}


def _qr_missing(df: pd.DataFrame) -> dict:
    n_total = len(df)
    n_cols  = df.shape[1]
    rows    = []
    for col in df.columns:
        n_missing = int(df[col].isna().sum())
        pct  = round(100 * n_missing / max(n_total, 1), 1)
        flag = "ok" if pct < 5 else ("warn" if pct < 20 else "critical")
        rows.append({"column": col, "n_missing": n_missing,
                     "pct_missing": pct, "quality_flag": flag})
    n_ok       = sum(1 for r in rows if r["quality_flag"] == "ok")
    n_warn     = sum(1 for r in rows if r["quality_flag"] == "warn")
    n_critical = sum(1 for r in rows if r["quality_flag"] == "critical")
    completeness = 100.0 if n_cols == 0 else round(
        100 * (n_ok + 0.5 * n_warn) / n_cols, 1)
    by_column    = sorted([r for r in rows if r["n_missing"] > 0],
                           key=lambda r: -r["n_missing"])
    total_missing = sum(r["n_missing"] for r in rows)
    return {"by_column": by_column,
            "n_complete_cols": sum(1 for r in rows if r["n_missing"] == 0),
            "n_warn_cols": n_warn, "n_critical_cols": n_critical,
            "completeness_score": completeness,
            "overall_pct_missing": round(100 * total_missing / max(n_total * n_cols, 1), 1)}


def _qr_demographics(df: pd.DataFrame) -> dict:
    out: dict[str, Any] = {}
    n = len(df)
    sex_col = _qr_col(df, ["sex", "sexo", "SEXO", "CS_SEXO"])
    if sex_col:
        out["sex"] = {"col": sex_col, "freq": _freq_tbl(df, sex_col),
                      "pct_missing": round(100 * int(df[sex_col].isna().sum()) / max(n, 1), 1)}
    race_col = _qr_col(df, ["race", "raca", "raza", "RACACOR", "RACA_COR", "CS_RACA"])
    if race_col:
        out["race"] = {"col": race_col, "freq": _freq_tbl(df, race_col),
                       "pct_missing": round(100 * int(df[race_col].isna().sum()) / max(n, 1), 1)}
    age_col = _qr_col(df, ["age_years", "idade_anos", "edad_anios",
                            "idade", "edad", "NU_IDADE_N", "IDADE"])
    if age_col:
        vals = pd.to_numeric(df[age_col], errors="coerce").dropna()
        out["age"] = {
            "col": age_col,
            "n_missing": int(df[age_col].isna().sum()),
            "pct_missing": round(100 * int(df[age_col].isna().sum()) / max(n, 1), 1),
            "min": float(vals.min()) if len(vals) else None,
            "max": float(vals.max()) if len(vals) else None,
            "mean": round(float(vals.mean()), 1) if len(vals) else None,
            "median": float(vals.median()) if len(vals) else None,
            "n_implausible": int(((vals < 0) | (vals > 130)).sum()),
        }
    edu_col = _qr_col(df, ["education_level", "education", "escolaridade",
                            "escolaridad", "ESC", "ESC2010", "CS_ESCOL_N"])
    if edu_col:
        out["education"] = {"col": edu_col, "freq": _freq_tbl(df, edu_col),
                            "pct_missing": round(100 * int(df[edu_col].isna().sum()) / max(n, 1), 1)}
    crisk_col = _qr_col(df, ["climate_risk_group", "grupo_risco_climatico", "grupo_riesgo_climatico"])
    if crisk_col:
        out["climate_risk"] = {"col": crisk_col, "freq": _freq_tbl(df, crisk_col),
                               "pct_missing": round(100 * int(df[crisk_col].isna().sum()) / max(n, 1), 1)}
    return out


def _qr_dates(df: pd.DataFrame) -> dict:
    out: dict[str, Any] = {}
    today  = pd.Timestamp.today().normalize()
    pre1900 = pd.Timestamp("1900-01-01")
    for col in df.columns:
        if not _is_datelike(df[col]):
            continue
        dates = pd.to_datetime(df[col], errors="coerce")
        valid = dates.dropna()
        out[col] = {
            "n_total": int(len(dates)),
            "n_missing": int(dates.isna().sum()),
            "n_future": int((valid > today).sum()),
            "n_pre1900": int((valid < pre1900).sum()),
            "date_min": str(valid.min().date()) if len(valid) else None,
            "date_max": str(valid.max().date()) if len(valid) else None,
        }
    return out


def _qr_icd(df: pd.DataFrame, top_n: int) -> dict:
    icd_col = _qr_col(df, ["underlying_cause", "causa_basica", "CAUSABAS",
                            "primary_diagnosis", "diagnostico_principal", "DIAG_PRINC",
                            "PA_CIDPRI", "notification_icd", "cid_principal", "cid10"])
    if icd_col is None:
        return {}
    codes    = df[icd_col]
    n_total  = len(codes)
    s        = codes.astype("string")
    is_valid = s.str.match(r"^[A-Za-z][0-9]{2}").fillna(False) & s.notna()
    n_missing = int(codes.isna().sum())
    n_valid   = int(is_valid.sum())
    vc  = codes.value_counts(dropna=False)
    top = sorted(
        [{"code": None if pd.isna(c) else c, "count": int(v),
          "pct": round(100 * int(v) / max(n_total, 1), 2)} for c, v in vc.items()],
        key=lambda r: (-r["count"], str(r["code"]))
    )[:top_n]
    chapters: list[dict] = []
    valid_codes = s[is_valid]
    if len(valid_codes):
        chap     = valid_codes.str[0].value_counts()
        chapters = sorted(
            [{"chapter": ch, "count": int(c)} for ch, c in chap.items()],
            key=lambda r: (-r["count"], r["chapter"])
        )
    return {
        "col": icd_col, "n_unique": int(codes.dropna().nunique()),
        "n_missing": n_missing,
        "pct_missing": round(100 * n_missing / max(n_total, 1), 1),
        "pct_valid": round(100 * n_valid / max(n_total, 1), 1),
        "top_codes": top, "chapters": chapters,
    }


def _qr_geographic(df: pd.DataFrame, top_n: int) -> dict:
    out: dict[str, Any] = {}
    n = len(df)
    muni_col = _qr_col(df, [
        "codigo_municipio_residencia", "residence_municipality_code",
        "codigo_municipio_ocorrencia", "occurrence_municipality_code",
        "notification_municipality_code", "municipality_code",
        "code_muni", "CODMUNRES", "MUNI_RES", "municipio_residencia",
    ])
    if muni_col:
        vc  = df[muni_col].value_counts(dropna=False)
        top = sorted(
            [{"code": None if pd.isna(k) else k, "count": int(v),
              "pct": round(100 * int(v) / max(n, 1), 2)} for k, v in vc.items()],
            key=lambda r: (-r["count"], str(r["code"]))
        )[:top_n]
        out["municipalities"] = {
            "col": muni_col,
            "n_unique": int(df[muni_col].dropna().nunique()),
            "n_missing": int(df[muni_col].isna().sum()),
            "top": top,
        }
    uf_col = _qr_col(df, [
        "uf_residencia", "residence_uf", "uf_ocorrencia", "occurrence_uf",
        "uf_notificacao", "notification_uf", "manager_uf", "uf_gestor",
        "UF_ZI", "SG_UF_NOT", "CODUFRES",
    ])
    if uf_col:
        vc  = df[uf_col].value_counts(dropna=False)
        top = sorted(
            [{"uf": None if pd.isna(k) else k, "count": int(v),
              "pct": round(100 * int(v) / max(n, 1), 2)} for k, v in vc.items()],
            key=lambda r: (-r["count"], str(r["uf"]))
        )[:top_n]
        out["states"] = {
            "col": uf_col,
            "n_unique": int(df[uf_col].dropna().nunique()),
            "top": top,
        }
    return out


def _qr_derived(df: pd.DataFrame, stage: str) -> dict:
    expected = {
        "age_group":        ["age_group", "faixa_etaria", "grupo_edad"],
        "ibge_age_group":   ["ibge_age_group", "faixa_etaria_ibge", "grupo_edad_ibge"],
        "climate_risk_grp": ["climate_risk_group", "grupo_risco_climatico", "grupo_riesgo_climatico"],
        "month":            ["month", "mes"],
        "year":             ["year", "ano", "anio"],
        "quarter":          ["quarter", "trimestre"],
        "epi_week":         ["epidemiological_week", "semana_epidemiologica"],
        "season":           ["astronomical_season", "estacao_astronomica", "estacion_astronomica",
                             "climatic_season", "estacao_climatica", "estacion_climatica"],
        "dry_rainy":        ["dry_rainy_season", "estacao_seca_chuvosa", "estacion_seca_lluviosa"],
    }
    presence = {}
    for key, patterns in expected.items():
        found = _qr_col(df, patterns)
        presence[key] = {"present": found is not None, "col": found}
    return {"variables": presence}


def _qr_score(report: dict) -> float | None:
    scores: list[float] = []
    weights: list[float] = []
    cs = report["missing"].get("completeness_score")
    if cs is not None:
        scores.append(cs); weights.append(0.4)
    demo_miss = [d.get("pct_missing", 0) or 0 for d in report["demographics"].values()]
    if demo_miss:
        scores.append(max(100 - sum(demo_miss) / len(demo_miss), 0)); weights.append(0.2)
    pv = report["icd"].get("pct_valid") if report["icd"] else None
    if pv is not None:
        scores.append(pv); weights.append(0.2)
    if report["dates"]:
        n_issues = sum(d["n_future"] + d["n_pre1900"] for d in report["dates"].values())
        n_tot    = sum(d["n_total"] for d in report["dates"].values())
        if n_tot > 0:
            scores.append(max(100 * (1 - n_issues / n_tot), 0)); weights.append(0.2)
    if not scores:
        return None
    return round(sum(s * w for s, w in zip(scores, weights)) / sum(weights), 1)

# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def _print_console(report: dict, lang: str) -> None:
    try:
        from rich.console import Console
        from rich.rule import Rule
        c = Console()
        _rich = True
    except ImportError:
        _rich = False

    def pr(text: str = "") -> None:
        if _rich:
            c.print(text)
        else:
            # strip rich markup
            clean = re.sub(r"\[/?[^\]]+\]", "", text)
            print(clean)

    def flag(f: str) -> str:
        if not _rich:
            return _FLAG_TXT.get(f, "")
        color = {"ok": "green", "warn": "yellow", "critical": "red", "info": "cyan"}.get(f, "cyan")
        return f"[{color}]{_FLAG_TXT[f]}[/]"

    meta, ov, miss = report["meta"], report["overview"], report["missing"]
    sc = report["score"]

    if _rich:
        c.print(Rule(f"[bold]{_qrl('report_title', lang)}[/]", align="left"))
    else:
        print(f"\n{'='*60}")
        print(_qrl('report_title', lang))
        print('='*60)

    pr(_qrl("generated_at", lang, meta["generated"]))
    score_str = "N/A" if sc is None else f"{round(sc, 1)} / 100"
    sflag = "info" if sc is None else ("ok" if sc >= 80 else ("warn" if sc >= 60 else "critical"))
    pr(f"{flag(sflag)}  {_qrl('quality_score', lang)}: [bold]{score_str}[/]" if _rich
       else f"{_FLAG_TXT[sflag]}  {_qrl('quality_score', lang)}: {score_str}")
    pr()

    # Pipeline
    pr(f"[bold cyan]{_qrl('sec_pipeline', lang)}[/]" if _rich else _qrl('sec_pipeline', lang))
    pr(f"{_qrl('current_stage', lang)}: {meta['stage']}  |  "
       f"{_qrl('system_label', lang)}: {meta['system']}  |  "
       f"{_qrl('type_label', lang)}: {meta['type']}")
    fns = report["pipeline"]["functions_applied"]
    if fns:
        pr(_qrl("fns_applied", lang))
        for fn in fns:
            pr(f"  [green]v[/]  {fn}" if _rich else f"  ✓  {fn}")
    else:
        pr(f"{_FLAG_TXT['warn']} {_qrl('no_history', lang)}")
    pr()

    # Overview
    pr(f"[bold cyan]{_qrl('sec_overview', lang)}[/]" if _rich else _qrl('sec_overview', lang))
    pr(f"{_qrl('ov_rows', lang)}: {ov['n_rows']:,}")
    pr(f"{_qrl('ov_cols', lang)}: {ov['n_cols']}")
    dflag = "ok" if ov["n_dup"] == 0 else ("warn" if ov["pct_dup"] < 1 else "critical")
    pr(f"{flag(dflag)}  {_qrl('ov_duplicates', lang)}: {ov['n_dup']} ({ov['pct_dup']}%)")
    pr()

    # Missing
    pr(f"[bold cyan]{_qrl('sec_missing', lang)}[/]" if _rich else _qrl('sec_missing', lang))
    pr(f"{_qrl('miss_complete_cols', lang)}: {miss['n_complete_cols']} / {ov['n_cols']}  |  "
       f"{_qrl('miss_warn', lang)}: {miss['n_warn_cols']}  |  "
       f"{_qrl('miss_critical', lang)}: {miss['n_critical_cols']}")
    pr(f"{_qrl('completeness_score', lang)}: {miss['completeness_score']} / 100")
    by_col = miss["by_column"]
    if by_col:
        for r in by_col[:10]:
            pr(f"  {flag(r['quality_flag'])}  {r['column']}: "
               f"{r['n_missing']} ({r['pct_missing']}%)")
        if len(by_col) > 10:
            pr(f"  ... ({len(by_col) - 10} {_qrl('more_cols', lang)})")
    else:
        pr(f"{_FLAG_TXT['ok']} {_qrl('no_missing', lang)}")
    pr()

    # Demographics
    pr(f"[bold cyan]{_qrl('sec_demographics', lang)}[/]" if _rich else _qrl('sec_demographics', lang))
    demo = report["demographics"]
    if not demo:
        pr(f"{_FLAG_TXT['warn']} {_qrl('no_demo_cols', lang)}")
    for key in ("sex", "race", "age", "education", "climate_risk"):
        d = demo.get(key)
        if not d:
            continue
        if key == "age":
            iflag = "ok" if d["n_implausible"] == 0 else "warn"
            pr(f"  {_qrl('age', lang)} [{d['col']}]: {d['min']}-{d['max']} | "
               f"{_qrl('mean', lang)} {d['mean']} | {_qrl('missing', lang)} {d['pct_missing']}% | "
               f"{flag(iflag)} {_qrl('implausible', lang)} {d['n_implausible']}")
        else:
            mf = "ok" if d["pct_missing"] < 5 else ("warn" if d["pct_missing"] < 20 else "critical")
            pr(f"  {_qrl(key, lang)} [{d['col']}]: {_qrl('missing', lang)} {d['pct_missing']}%  {flag(mf)}")
    pr()

    # Dates
    if report["dates"]:
        pr(f"[bold cyan]{_qrl('sec_dates', lang)}[/]" if _rich else _qrl('sec_dates', lang))
        for col, d in report["dates"].items():
            iflag = "ok" if (d["n_future"] + d["n_pre1900"]) == 0 else "warn"
            rng   = f"{d['date_min']} - {d['date_max']}" if d["date_min"] else "N/A"
            pr(f"  {flag(iflag)}  {col}: {rng}  |  "
               f"{_qrl('future', lang)} {d['n_future']}  |  "
               f"{_qrl('pre1900', lang)} {d['n_pre1900']}  |  "
               f"{_qrl('missing', lang)} {d['n_missing']}")
        pr()

    # ICD
    if report["icd"]:
        icd  = report["icd"]
        pr(f"[bold cyan]{_qrl('sec_icd', lang)}[/]" if _rich else _qrl('sec_icd', lang))
        vflag = "ok" if icd["pct_valid"] >= 95 else ("warn" if icd["pct_valid"] >= 80 else "critical")
        pr(f"  {flag(vflag)}  [{icd['col']}]  {_qrl('icd_unique', lang)} {icd['n_unique']}  |  "
           f"{_qrl('icd_valid', lang)} {icd['pct_valid']}%  |  "
           f"{_qrl('missing', lang)} {icd['pct_missing']}%")
        tops = [t for t in icd["top_codes"] if t["code"] is not None][:5]
        for t in tops:
            pr(f"    {t['code']}: {t['count']} ({t['pct']}%)")
        pr()

    # Geographic
    geo = report["geographic"]
    if geo:
        pr(f"[bold cyan]{_qrl('sec_geographic', lang)}[/]" if _rich else _qrl('sec_geographic', lang))
        if geo.get("municipalities"):
            m = geo["municipalities"]
            pr(f"  {_qrl('municipalities', lang)}: {m['n_unique']}  |  "
               f"{_qrl('missing', lang)} {m['n_missing']}")
        if geo.get("states"):
            pr(f"  {_qrl('states', lang)}: {geo['states']['n_unique']}")
        pr()

    # Derived
    pr(f"[bold cyan]{_qrl('sec_derived', lang)}[/]" if _rich else _qrl('sec_derived', lang))
    for key, v in report["derived"]["variables"].items():
        icon = ("[green]v[/]" if _rich else "✓") if v["present"] else ("[yellow]?[/]" if _rich else "?")
        lbl  = f"{key} [{v['col']}]" if v["present"] else key
        pr(f"  {icon}  {lbl}")

    if _rich:
        c.print(Rule())
    else:
        print('='*60)

# ---------------------------------------------------------------------------
# Markdown + HTML output
# ---------------------------------------------------------------------------

def _flag_md(f: str) -> str:
    return {"ok": "✅", "warn": "⚠️", "critical": "🔴", "info": "ℹ️"}.get(f, "ℹ️")


def _render_markdown(report: dict, lang: str) -> str:
    meta, ov, miss = report["meta"], report["overview"], report["missing"]
    pipe = report["pipeline"]
    sc   = report["score"]
    L: list[str] = [f"# {_qrl('report_title', lang)}", "",
                    f"> {_qrl('generated_at', lang, meta['generated'])}", ""]
    score_str = "N/A" if sc is None else f"{round(sc, 1)} / 100"
    sflag = "info" if sc is None else ("ok" if sc >= 80 else ("warn" if sc >= 60 else "critical"))
    L += [f"**{_qrl('quality_score', lang)}**: {_flag_md(sflag)} {score_str}", "", "---", ""]
    L += [f"## {_qrl('sec_pipeline', lang)}", "",
          f"| {_qrl('metric', lang)} | {_qrl('value', lang)} |", "|---|---|",
          f"| {_qrl('current_stage', lang)} | `{meta['stage']}` |",
          f"| {_qrl('system_label', lang)} | `{meta['system']}` |",
          f"| {_qrl('type_label', lang)} | `{meta['type']}` |",
          f"| {_qrl('history_entries', lang)} | {meta['n_history']} |", ""]
    if pipe["functions_applied"]:
        L += [f"### {_qrl('fns_applied', lang)}", ""]
        for fn in pipe["functions_applied"]:
            L.append(f"- `{fn}`")
        L.append("")
    if pipe["parsed_steps"]:
        L += [f"### {_qrl('history_log', lang)}", ""]
        for step in pipe["parsed_steps"]:
            ts, msg = step["timestamp"], step["message"]
            L.append(f"- [{ts}]  {msg}" if ts else f"- {msg}")
        L.append("")
    L += ["---", "", f"## {_qrl('sec_overview', lang)}", "",
          f"- **{_qrl('ov_rows', lang)}**: {ov['n_rows']:,}",
          f"- **{_qrl('ov_cols', lang)}**: {ov['n_cols']}"]
    dup_flag = "ok" if ov["n_dup"] == 0 else ("warn" if ov["pct_dup"] < 1 else "critical")
    L += [f"- {_flag_md(dup_flag)} **{_qrl('ov_duplicates', lang)}**: {ov['n_dup']} ({ov['pct_dup']}%)",
          "", "---", "", f"## {_qrl('sec_missing', lang)}", ""]
    sc_miss = miss["completeness_score"]
    sc_flag = "ok" if sc_miss >= 80 else ("warn" if sc_miss >= 60 else "critical")
    L += [f"**{_qrl('completeness_score', lang)}**: {_flag_md(sc_flag)} {sc_miss} / 100", ""]
    if miss["by_column"]:
        L += [f"| {_qrl('column', lang)} | n_missing | pct_missing | flag |", "|---|---|---|---|"]
        for r in miss["by_column"]:
            L.append(f"| {r['column']} | {r['n_missing']} | {r['pct_missing']} | {r['quality_flag']} |")
        L.append("")
    else:
        L += [f"> {_qrl('no_missing', lang)}", ""]
    L += ["---", "", f"## {_qrl('sec_derived', lang)}", ""]
    for key, v in report["derived"]["variables"].items():
        flg = "ok" if v["present"] else "warn"
        lbl = f"{key} [{v['col']}]" if v["present"] else key
        L.append(f"- {_flag_md(flg)} {lbl}")
    L.append("")
    return "\n".join(L) + "\n"


def _render_html(report: dict, lang: str) -> str:
    meta, ov, miss = report["meta"], report["overview"], report["missing"]
    pipe = report["pipeline"]
    sc   = report["score"]
    score_str   = "N/A" if sc is None else f"{round(sc, 1)} / 100"
    score_color = ("#2980b9" if sc is None else
                   "#27ae60" if sc >= 80 else
                   "#e67e22" if sc >= 60 else "#e74c3c")

    def badge(status: str, text: str) -> str:
        col = {"ok":"#27ae60","warn":"#e67e22","critical":"#e74c3c","info":"#2980b9"}.get(status,"#2980b9")
        return (f'<span style="background:{col};color:#fff;border-radius:4px;'
                f'padding:2px 7px;font-size:11px">{text}</span>')

    miss_rows = ""
    for r in miss["by_column"]:
        bg = {"ok":"#eafaf1","warn":"#fef9e7","critical":"#fdf0ed"}.get(r["quality_flag"],"#fff")
        miss_rows += (f'<tr style="background:{bg}"><td>{r["column"]}</td>'
                      f'<td>{r["n_missing"]}</td><td>{r["pct_missing"]}%</td>'
                      f'<td>{badge(r["quality_flag"], r["quality_flag"])}</td></tr>')

    fns_html  = "".join(f"<li>{fn}</li>" for fn in pipe["functions_applied"])
    hist_html = "".join(
        f'<div style="font-size:12px;color:#555;margin:2px 0">[{s["timestamp"]}] {s["message"]}</div>'
        if s["timestamp"] else
        f'<div style="font-size:12px;color:#555;margin:2px 0">{s["message"]}</div>'
        for s in pipe["parsed_steps"]
    )

    drv_rows = ""
    for key, v in report["derived"]["variables"].items():
        st = "ok" if v["present"] else "warn"
        drv_rows += (f"<tr><td>{key}</td>"
                     f"<td>{badge(st, _qrl('present' if v['present'] else 'absent', lang))}</td>"
                     f"<td>{v['col'] or '-'}</td></tr>")

    css = """
body{font-family:Arial,sans-serif;margin:40px;color:#2c3e50;max-width:1100px}
h1{color:#1a252f;border-bottom:3px solid #1B6CA8;padding-bottom:8px}
h2{color:#1B6CA8;border-bottom:1px solid #dde;margin-top:30px}
h3{color:#34495e;margin-top:15px}
table{border-collapse:collapse;width:100%;margin:12px 0;font-size:13px}
th,td{border:1px solid #dde;padding:6px 10px;text-align:left}
th{background:#1B6CA8;color:#fff}
tr:nth-child(even){background:#f8f9fa}
.score-box{display:inline-block;padding:10px 20px;border-radius:6px;
  font-size:22px;font-weight:bold;color:#fff;margin:8px 0}
.meta-bar{background:#f0f4f8;border-radius:6px;padding:12px 18px;
  margin-bottom:20px;font-size:13px;display:flex;gap:24px;flex-wrap:wrap}
.fn-list{list-style:none;padding:0}
.fn-list li::before{content:"✓  ";color:#27ae60;font-weight:bold}
footer{margin-top:40px;font-size:12px;color:#aaa;border-top:1px solid #eee;padding-top:10px}
"""
    return f"""<!DOCTYPE html><html lang="{lang}"><head><meta charset="UTF-8">
<title>{_qrl('report_title', lang)}</title><style>{css}</style></head><body>
<h1>{_qrl('report_title', lang)}</h1>
<div class="meta-bar">
  <div>{_qrl('generated_at', lang, meta['generated'])}</div>
  <div>{_qrl('current_stage', lang)}: <b>{meta['stage']}</b></div>
  <div>{_qrl('system_label', lang)}: <b>{meta['system']}</b></div>
  <div>{_qrl('type_label', lang)}: <b>{meta['type']}</b></div>
</div>
<div class="score-box" style="background:{score_color}">{_qrl('quality_score', lang)}: {score_str}</div>
<h2>{_qrl('sec_pipeline', lang)}</h2>
<ul class="fn-list">{fns_html}</ul>
{('<h3>' + _qrl('history_log', lang) + '</h3>' + hist_html) if hist_html else ''}
<h2>{_qrl('sec_overview', lang)}</h2>
<table><thead><tr><th>{_qrl('metric', lang)}</th><th>{_qrl('value', lang)}</th></tr></thead>
<tbody>
<tr><td>{_qrl('ov_rows', lang)}</td><td>{ov['n_rows']:,}</td></tr>
<tr><td>{_qrl('ov_cols', lang)}</td><td>{ov['n_cols']}</td></tr>
<tr><td>{_qrl('ov_duplicates', lang)}</td><td>{ov['n_dup']} ({ov['pct_dup']}%)</td></tr>
</tbody></table>
<h2>{_qrl('sec_missing', lang)}</h2>
<p><b>{_qrl('completeness_score', lang)}:</b>
{badge('ok' if miss['completeness_score']>=80 else 'warn' if miss['completeness_score']>=60 else 'critical',
       str(miss['completeness_score']) + ' / 100')}</p>
<table><thead><tr><th>{_qrl('column', lang)}</th><th>N missing</th><th>%</th>
<th>{_qrl('status', lang)}</th></tr></thead><tbody>{miss_rows}</tbody></table>
<h2>{_qrl('sec_derived', lang)}</h2>
<table><thead><tr><th>{_qrl('variable', lang)}</th><th>{_qrl('status', lang)}</th>
<th>{_qrl('column', lang)}</th></tr></thead><tbody>{drv_rows}</tbody></table>
<footer>climasus4py | {_qrl('source_note', lang)}</footer>
</body></html>"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_quality_report(
    data: Any,
    *,
    output_format: str = "console",
    output_file: str | None = None,
    check_icd: bool = True,
    check_dates: bool = True,
    top_n: int = 10,
    lang: str = "pt",
    verbose: bool = True,
) -> dict:
    """Comprehensive data quality report for a SUS dataset.

    Mirrors ``climasus4r::sus_data_quality_report()``.

    Sections: pipeline audit, overview, missing values, demographics,
    dates, ICD-10 codes, geographic coverage, derived variables,
    weighted quality score.

    Args:
        data: ``pandas.DataFrame`` or lazy ``DuckDBPyRelation``
            (materialised at the API boundary).
        output_format: ``"console"`` (default), ``"markdown"``,
            ``"html"``. ``"gt"`` is R-specific — not ported.
        output_file: Path for markdown/html output. Auto-named if None.
        check_icd: Compute ICD-10 quality section.
        check_dates: Compute date validation section.
        top_n: Rows in top-frequency tables.
        lang: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages.

    Returns:
        Report ``dict`` with keys:
        ``meta, pipeline, overview, missing, demographics, dates,
        icd, geographic, derived, score``.

    Example:
        >>> import climasus4py as cs
        >>> rel   = cs.sus_data_import("SIM-DO", "SE", 2023)
        >>> stand = cs.sus_data_standardize(cs.sus_data_clean_encoding(rel))
        >>> report = cs.sus_data_quality_report(stand, lang="en")
        >>> report["score"]
        87.3
        >>> cs.sus_data_quality_report(stand, output_format="html",
        ...     output_file="quality_report.html")
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    if output_format not in ("console", "markdown", "html"):
        raise ValueError("output_format must be 'console', 'markdown' or 'html'.")

    meta_raw = _extract_meta(data)
    df       = _as_dataframe(data)

    if verbose:
        print(_qrl("computing", lang))

    stage   = meta_raw["stage"]
    history = meta_raw["history"]

    report: dict[str, Any] = {
        "meta": {
            "stage":     stage,
            "system":    meta_raw["system"],
            "type":      meta_raw["type"],
            "backend":   meta_raw["backend"],
            "n_history": len(history),
            "generated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "pipeline":     _qr_parse_history(history, stage),
        "overview":     _qr_overview(df),
        "missing":      _qr_missing(df),
        "demographics": _qr_demographics(df),
        "dates":        _qr_dates(df) if check_dates else {},
        "icd":          _qr_icd(df, top_n) if check_icd else {},
        "geographic":   _qr_geographic(df, top_n),
        "derived":      _qr_derived(df, stage),
        "score":        None,
    }
    report["score"] = _qr_score(report)

    if output_format == "console":
        _print_console(report, lang)
    elif output_format in ("markdown", "html"):
        if output_file is None:
            ts  = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            ext = "html" if output_format == "html" else "md"
            output_file = f"dq_report_{ts}.{ext}"
        path = Path(output_file)
        if output_format == "markdown":
            path.write_text(_render_markdown(report, lang), encoding="utf-8")
        else:
            path.write_text(_render_html(report, lang), encoding="utf-8")
        if verbose:
            print(_qrl("saved_to", lang, str(path)))

    return report
