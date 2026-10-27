#!/usr/bin/env bash
# Load deployment identity without executing the local configuration file.

rffusion_load_environment() {
    # Args: repository root and component (appcataloga, webfusion or mariadb).
    # Sets rffusion_identity_args for podman run; returns 1 on invalid input.
    local repo_root="$1" component="$2"
    local config_path="${repo_root}/.env/runtime.env"
    local line environment_value="" suffix

    case "$component" in
        appcataloga|webfusion|mariadb) ;;
        *) printf 'ERROR: Unknown RF.Fusion component.\n' >&2; return 1 ;;
    esac

    if [[ ! -f "$config_path" || ! -r "$config_path" ]]; then
        printf 'ERROR: Configure %s using .env/README.md before deploying.\n' "$config_path" >&2
        return 1
    fi

    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ "$line" =~ ^[[:space:]]*(#.*)?$ ]] && continue
        if [[ ! "$line" =~ ^[[:space:]]*RFFUSION_ENV[[:space:]]*=[[:space:]]*(production|development)[[:space:]]*$ ]]; then
            printf 'ERROR: Invalid environment configuration. See .env/README.md.\n' >&2
            return 1
        fi
        if [[ -n "$environment_value" ]]; then
            printf 'ERROR: RFFUSION_ENV must appear exactly once.\n' >&2
            return 1
        fi
        environment_value="${BASH_REMATCH[1]}"
    done < "$config_path"

    case "$environment_value" in
        production) suffix="prod" ;;
        development) suffix="dev" ;;
        *) printf 'ERROR: RFFUSION_ENV is required. See .env/README.md.\n' >&2; return 1 ;;
    esac

    rffusion_identity_args=(
        --hostname "${component}-${suffix}"
        --label "io.rffusion.environment=${environment_value}"
        --label "io.rffusion.component=${component}"
        -e "RFFUSION_ENV=${environment_value}"
        -e "RFFUSION_COMPONENT=${component}"
    )
    printf 'RF.Fusion: environment=%s component=%s hostname=%s-%s\n' \
        "$environment_value" "$component" "$component" "$suffix"
}
