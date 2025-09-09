#!/usr/bin/env bash
set -Eeuo pipefail

# ===================== CONFIG =====================
CONTAINER="${CONTAINER:-incendios-forestales}"
WORKDIR="${WORKDIR:-/home/rstudio/descarga-incendios-california}"
RSCRIPT="${RSCRIPT:-descargar-incendios-california.R}"

FROM_YEAR="${FROM_YEAR:-1960}"      # ./run_all_years_secuencial.sh 1960 1969
TO_YEAR="${TO_YEAR:-2024}"
KRIGR_CORES="${KRIGR_CORES:-1}"     # núcleos internos usados por KrigR dentro de R
TAIL_LINES="${TAIL_LINES:-60}"      # líneas a mostrar de logs fallidos
SHOW_LOG_CONSOLE="${SHOW_LOG_CONSOLE:-true}"  # true para ver el log del año en curso en consola

# ================== CLI OVERRIDES =================
if [[ $# -ge 1 ]]; then FROM_YEAR="$1"; fi
if [[ $# -ge 2 ]]; then TO_YEAR="$2"; fi

if ! command -v docker >/dev/null; then
  echo "ERROR: docker no está instalado o no está en PATH." >&2
  exit 1
fi

# ================== PREP / CLEAN ONCE =============
HOST_LOG_DIR="${HOST_LOG_DIR:-logs}"
mkdir -p "$HOST_LOG_DIR"

echo "→ Limpieza inicial en contenedor ($CONTAINER:$WORKDIR): salidas/ y tmp/"
docker exec -w "$WORKDIR" "$CONTAINER" bash -lc 'rm -rf salidas tmp && mkdir -p salidas tmp'

YEARS=$(seq "$FROM_YEAR" "$TO_YEAR")
echo "→ Ejecutando $RSCRIPT secuencialmente para años ${FROM_YEAR}..${TO_YEAR}"
echo "   (KRIGR_CORES=$KRIGR_CORES dentro de cada ejecución)"
echo

# ================== EJECUCIÓN SECUENCIAL ==========
ok_years=()
failed_years=()

for y in $YEARS; do
  log="$HOST_LOG_DIR/$y.log"
  st="$HOST_LOG_DIR/$y.status"

  echo "— Año $y ..."
  start_ts=$(date +%Y-%m-%dT%H:%M:%S)
  {
    echo "== INICIO $start_ts =="
    echo "Comando: Rscript --vanilla $RSCRIPT --year-start $y --year-end $y"
  } > "$log"

  # Ejecutar dentro del contenedor y registrar exit code
  set +e
  if [[ "$SHOW_LOG_CONSOLE" == "true" ]]; then
    # Muestra el log del año en curso a la consola y también a archivo
    docker exec -w "$WORKDIR" "$CONTAINER" bash -lc \
      "KRIGR_CORES='$KRIGR_CORES' Rscript --vanilla '$RSCRIPT' --year-start '$y' --year-end '$y'" \
      2>&1 | tee -a "$log"
    exit_code=${PIPESTATUS[0]}
  else
    # Solo a archivo (consola silenciosa durante la ejecución)
    docker exec -w "$WORKDIR" "$CONTAINER" bash -lc \
      "KRIGR_CORES='$KRIGR_CORES' Rscript --vanilla '$RSCRIPT' --year-start '$y' --year-end '$y'" \
      >> "$log" 2>&1
    exit_code=$?
  fi
  set -e

  end_ts=$(date +%Y-%m-%dT%H:%M:%S)
  {
    echo "== FIN $end_ts (exit=$exit_code) =="
  } >> "$log"
  echo "$exit_code" > "$st"

  if [[ "$exit_code" == "0" ]]; then
    ok_years+=("$y")
    echo "  ✓ Año $y OK"
  else
    failed_years+=("$y")
    echo "  ✗ Año $y FALLÓ (ver $log)"
  fi
  echo
done

# ================== RESUMEN =======================
echo
echo "================== RESUMEN =================="
echo "Años OK     (${#ok_years[@]}): ${ok_years[*]:-—}"
echo "Años fallo  (${#failed_years[@]}): ${failed_years[*]:-—}"
echo "Logs en: $HOST_LOG_DIR/"
echo "============================================"
echo

if [[ ${#failed_years[@]} -gt 0 ]]; then
  echo "Detalles de fallos (últimas ${TAIL_LINES} líneas por año):"
  echo
  for y in "${failed_years[@]}"; do
    echo "----- Año $y -----"
    if [[ -f "$HOST_LOG_DIR/$y.log" ]]; then
      tail -n "$TAIL_LINES" "$HOST_LOG_DIR/$y.log"
    else
      echo "(no existe $HOST_LOG_DIR/$y.log)"
    fi
    echo
  done
fi

echo "✓ Finalizado."

