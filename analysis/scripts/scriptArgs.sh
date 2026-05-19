# scriptArgs.sh — source this file at the top of any pipeline shell script.
# Provides: parseScriptArgs, getDataLakeRoot, getWarehouseRoot,
#           getWarehouseFormat, getDaysBeforeInactive
#
# Precedence for all values: CLI flag > env var > .env file > error
# Precedence for .env location: --envFile > ENV_FILE env var > walk CWD to root

# ── .env loading ──────────────────────────────────────────────────────────────
# Only exports keys not already present in the environment, so a pre-existing
# env var always wins over a .env value for the same name.
_loadDotenv() {
    local env_file="${1:-}" key value
    if [[ -z "${env_file}" ]]; then
        local dir="${PWD}"
        while [[ "${dir}" != "/" ]]; do
            if [[ -f "${dir}/.env" ]]; then
                env_file="${dir}/.env"
                break
            fi
            dir="$(dirname "${dir}")"
        done
    fi
    [[ -n "${env_file}" && -f "${env_file}" ]] || return 0
    while IFS='=' read -r key value; do
        [[ "${key}" =~ ^[[:space:]]*(#|$) ]] && continue   # skip comments/blanks
        key="${key%%[[:space:]]*}"                           # trim key whitespace
        value="${value%\"}" ; value="${value#\"}"            # strip optional quotes
        [[ -z "${!key+x}" ]] && export "${key}=${value}"    # env var wins on conflict
    done < "${env_file}"
}

# ── lazy .env initialization ─────────────────────────────────────────────────
# Ensures .env is loaded exactly once — on the first getter call or when
# parseScriptArgs completes, whichever comes first.
_DOTENV_LOADED=0
_ensureDotenv() {
    (( _DOTENV_LOADED )) && return 0
    _loadDotenv "${_ARG_ENV_FILE:-${ENV_FILE:-}}"
    _DOTENV_LOADED=1
}

# ── CLI flag parsing ──────────────────────────────────────────────────────────
# Call parseScriptArgs "$@" immediately after sourcing this file.
# --envFile is resolved and consumed here; remaining flags populate _ARG_* vars.
# If parseScriptArgs is not called, getters trigger lazy loading automatically.
parseScriptArgs() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --envFile)            _ARG_ENV_FILE="$2";             shift 2 ;;
            --dataLakeRoot)       _ARG_DATA_LAKE_ROOT="$2";       shift 2 ;;
            --warehouseRoot)      _ARG_WAREHOUSE_ROOT="$2";       shift 2 ;;
            --warehouseFormat)    _ARG_WAREHOUSE_FORMAT="$2";     shift 2 ;;
            --daysBeforeInactive) _ARG_DAYS_BEFORE_INACTIVE="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    _ensureDotenv
}

# ── internal: infer orc|parquet from a path string ───────────────────────────
_inferWarehouseFormat() {
    local path="$1" part has_orc=0 has_parquet=0
    IFS='/' read -ra _parts <<< "${path}"
    for part in "${_parts[@]}"; do
        [[ "${part}" == "orc" ]]     && has_orc=1
        [[ "${part}" == "parquet" ]] && has_parquet=1
    done
    (( has_orc + has_parquet == 1 )) || return 1
    (( has_orc )) && echo "orc" || echo "parquet"
}

# ── getters ───────────────────────────────────────────────────────────────────

getDataLakeRoot() {
    _ensureDotenv
    [[ -n "${_ARG_DATA_LAKE_ROOT:-}" ]] && echo "${_ARG_DATA_LAKE_ROOT}" && return 0
    [[ -n "${DATA_LAKE_ROOT:-}"      ]] && echo "${DATA_LAKE_ROOT}"      && return 0
    echo "ERROR: DATA_LAKE_ROOT not resolved" \
         "(--dataLakeRoot | DATA_LAKE_ROOT | .env:DATA_LAKE_ROOT)" >&2
    return 1
}

getWarehouseRoot() {
    _ensureDotenv
    [[ -n "${_ARG_WAREHOUSE_ROOT:-}" ]] && echo "${_ARG_WAREHOUSE_ROOT}" && return 0
    [[ -n "${WAREHOUSE_ROOT:-}"      ]] && echo "${WAREHOUSE_ROOT}"      && return 0
    echo "ERROR: WAREHOUSE_ROOT not resolved" \
         "(--warehouseRoot | WAREHOUSE_ROOT | .env:WAREHOUSE_ROOT)" >&2
    return 1
}

getWarehouseFormat() {
    _ensureDotenv
    [[ -n "${_ARG_WAREHOUSE_FORMAT:-}" ]] && echo "${_ARG_WAREHOUSE_FORMAT}" && return 0
    [[ -n "${WAREHOUSE_FORMAT:-}"      ]] && echo "${WAREHOUSE_FORMAT}"      && return 0
    local root inferred
    root="$(getWarehouseRoot 2>/dev/null)" \
        && inferred="$(_inferWarehouseFormat "${root}")" \
        && echo "${inferred}" && return 0
    echo "ERROR: WAREHOUSE_FORMAT not resolved" \
         "(--warehouseFormat | WAREHOUSE_FORMAT | .env:WAREHOUSE_FORMAT | orc|parquet in WAREHOUSE_ROOT)" >&2
    return 1
}

getDaysBeforeInactive() {
    _ensureDotenv
    [[ -n "${_ARG_DAYS_BEFORE_INACTIVE:-}" ]] && echo "${_ARG_DAYS_BEFORE_INACTIVE}" && return 0
    [[ -n "${DAYS_BEFORE_INACTIVE:-}"      ]] && echo "${DAYS_BEFORE_INACTIVE}"      && return 0
    echo "ERROR: DAYS_BEFORE_INACTIVE not resolved" \
         "(--daysBeforeInactive | DAYS_BEFORE_INACTIVE | .env:DAYS_BEFORE_INACTIVE)" >&2
    return 1
}
