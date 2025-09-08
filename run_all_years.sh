#!/usr/bin/env bash
set -Eeuo pipefail

# ===================== CONFIG =====================
CONTAINER="${CONTAINER:-incendios-forestales}"
WORKDIR="${WORKDIR:-/home/rstudio/descarga-incendios-california}"
RSCRIPT="${RSCRIPT:-descargar-incendios-california.R}"

FROM_YEAR="${FROM_YEAR:-1960}"      # ./run_all_years.sh 1960 2024 8
TO_YEAR="${TO_YEAR:-2024}"
PARALLELISM="${PARALLELISM:-8}"     # nº de trabajos simultáneos
KRIGR_CORES="${KRIGR_CORES:-1}"     # núcleos internos por job
TAIL_LINES="${TAIL_LINES:-60}"      # líneas a mostrar de logs fallidos

# ================== CLI OVERRIDES =================
if [[ $# -ge 1 ]]; then FROM_YEAR="$1"; fi
if [[ $# -ge 2 ]]; then TO_YEAR="$2"; fi
if [[ $# -ge 3 ]]; then PARALLELISM="$3"; fi

if ! command -v docker >/dev/null; then
  echo "ERROR: docker no está instalado o no está en PATH." >&2
  exit 1
fi

# ================== PREP / CLEAN ONCE =============
HOST_LOG_DIR="${HOST_LOG_DIR:-logs}"
mkdir -p "$HOST_LOG_DIR"

echo "→ Limpieza inicial en contenedor ($CONTAINER:$WORKDIR): salidas/ y tmp/"
docker exec -w "$WORKDIR" "$CONTAINER" bash -lc 'rm -rf salidas tmp && mkdir -p salidas tmp'

echo "→ Ejecutando $RSCRIPT para años ${FROM_YEAR}..${TO_YEAR} con concurrencia $PARALLELISM"
echo "   (KRIGR_CORES=$KRIGR_CORES dentro de cada job)"
YEARS=$(seq "$FROM_YEAR" "$TO_YEAR")

# ================== DISPATCH ======================
run_cmd_for_year() {
  local y="$1"
  # Ejecuta dentro del contenedor y registra status en el host
  docker exec -w "$WORKDIR" "$CONTAINER" bash -lc \
    "KRIGR_CORES='$KRIGR_CORES' Rscript --vanilla '$RSCRIPT' --year-start '$y' --year-end '$y'"
}
export -f run_cmd_for_year
export CONTAINER WORKDIR RSCRIPT KRIGR_CORES HOST_LOG_DIR

if command -v parallel >/dev/null 2>&1; then
  # ---- GNU parallel ---- (no detenemos al primer fallo)
  set +e
  echo "$YEARS" | parallel -j "$PARALLELISM" \
    "bash -lc 'run_cmd_for_year {}' > '${HOST_LOG_DIR}'/{}.log 2>&1; echo \$? > '${HOST_LOG_DIR}'/{}.status"
  PAR_STATUS=$?
  set -e
  echo "(parallel exit=$PAR_STATUS; continuando con el resumen por status files)"
elif command -v xargs >/dev/null 2>&1; then
  # ---- xargs -P ----
  set +e
  echo "$YEARS" | xargs -n1 -P "$PARALLELISM" -I{} bash -lc \
    "docker exec -w '$WORKDIR' '$CONTAINER' bash -lc 'KRIGR_CORES=\"$KRIGR_CORES\" Rscript --vanilla \"$RSCRIPT\" --year-start {} --year-end {}' > '$HOST_LOG_DIR'/{}.log 2>&1; echo \$? > '$HOST_LOG_DIR'/{}.status"
  XARGS_STATUS=$?
  set -e
  echo "(xargs exit=$XARGS_STATUS; continuando con el resumen por status files)"
else
  # ---- Fallback: jobs en Bash ----
  echo "Aviso: ni 'parallel' ni 'xargs' disponibles; usando jobs de Bash."
  set +e
  running=0
  for y in $YEARS; do
    (
      run_cmd_for_year "$y" > "$HOST_LOG_DIR/$y.log" 2>&1
      echo $? > "$HOST_LOG_DIR/$y.status"
    ) &
    (( running+=1 ))
    if [[ "$running" -ge "$PARALLELISM" ]]; then
      wait -n
      (( running-=1 ))
    fi
  done
  wait
  set -e
fi

# ================== SUMMARY =======================
ok_years=()
failed_years=()

for y in $YEARS; do
  st_file="$HOST_LOG_DIR/$y.status"
  if [[ -f "$st_file" ]] && [[ "$(cat "$st_file")" == "0" ]]; then
    ok_years+=("$y")
  else
    failed_years+=("$y")
  fi
done

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
