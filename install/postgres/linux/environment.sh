#!/usr/bin/env bash

postgres_error() {
    printf 'ERROR postgres: %s\n' "$*" >&2
    return 1
}

postgres_load_environment() {
    local file="$1/.env/runtime.env" line value=""
    [[ -r "$file" && -f "$file" ]] || { postgres_error "Configure $file."; return 1; }
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ "$line" =~ ^[[:space:]]*(#.*)?$ ]] && continue
        [[ "$line" =~ ^[[:space:]]*RFFUSION_ENV[[:space:]]*=[[:space:]]*(development|production)[[:space:]]*$ ]] ||
            { postgres_error "Invalid runtime.env."; return 1; }
        [[ -z "$value" ]] || { postgres_error "Duplicate RFFUSION_ENV."; return 1; }
        value="${BASH_REMATCH[1]}"
    done < "$file"
    [[ -n "$value" ]] || { postgres_error "Missing RFFUSION_ENV."; return 1; }
    export RFFUSION_ENV="$value" RFFUSION_COMPONENT=postgres
}

postgres_validate_identity() {
    [[ "${RFFUSION_COMPONENT:-}" == postgres ]] ||
        { postgres_error "Invalid component."; return 1; }
    case "${RFFUSION_ENV:-}" in
        production) POSTGRES_HOSTNAME=postgres-prod ;;
        development) POSTGRES_HOSTNAME=postgres-dev ;;
        *) postgres_error "Invalid environment."; return 1 ;;
    esac
}

postgres_prompt_profile() {
    postgres_validate_identity || return 1
    printf 'case $- in *i*)\n case "${PS1-}" in\n "[RF.Fusion %s postgres] "*) ;;\n *) PS1="[RF.Fusion %s postgres] ${PS1-}" ;;\n esac\nesac\n' \
        "$RFFUSION_ENV" "$RFFUSION_ENV"
}

postgres_install_identity() {
    postgres_validate_identity || return 1
    printf 'RFFUSION_ENV=%s\nRFFUSION_COMPONENT=postgres\n' "$RFFUSION_ENV" > /etc/rffusion-environment
    printf 'RF.Fusion: environment=%s component=postgres hostname=%s\n' "$RFFUSION_ENV" "$POSTGRES_HOSTNAME"
    postgres_prompt_profile > /etc/profile.d/rffusion-postgres.sh
    if ! grep -Fq '/etc/profile.d/rffusion-postgres.sh' /etc/bash.bashrc; then
        printf '\n. /etc/profile.d/rffusion-postgres.sh\n' >> /etc/bash.bashrc
    fi
}
