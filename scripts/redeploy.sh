#!/bin/bash
# redeploy.sh — build, recreate and health-gate a named list of services, then
# release deploy intent.
#
# Usage:
#   bash scripts/redeploy.sh <service> [service ...]
#   bash scripts/redeploy.sh --restart <service> [service ...]   (--config: same)
#
# Run from the compose project directory (/opt/cartracker on the production VM).
#
# Pick the mode by what has to change:
#
#   default    New code. Builds the image and recreates the container. Does
#              nothing at all when Compose sees no drift, and says so.
#   --restart  The *process* has to restart, but its image and service config
#              have not changed. Two known reasons: a bind-mounted config file
#              (decision 4) and a cached peer address (decision 5).
#
# ---------------------------------------------------------------------------
# Plan 144 — the four decisions this script encodes. Each replaced a defect
# observed on a real production deploy on 2026-08-20.
#
# 1. `up -d --no-deps`. Without it Compose walks the dependency graph and
#    re-runs work nobody asked for — a deploy of `ops archiver pack-worker
#    processing` re-ran `flyway`. Dependencies are therefore *checked, never
#    recreated*: the check is `docker compose ps`, printed on failure, which
#    shows the fleet state at the moment the deploy broke. It is deliberately
#    not a pre-flight gate. Healthchecks here are shallow by contract (they
#    never probe another container), so a target going healthy proves nothing
#    about its dependencies; and gating a deploy on unrelated unhealthy
#    services would block deploying the fix during the incident that needs it.
#
# 2. A health gate, not `sleep 10`. Ten seconds is not a readiness contract —
#    `loki` and `pgadmin` were both observed still `starting` past that mark.
#    Every recreated, pollable service must reach `healthy` or the deploy
#    fails loudly. The default timeout is 300s because the worst case any
#    healthcheck in docker-compose.yml can take is `start_period + retries *
#    (interval + timeout)` = 30 + 5 * (30 + 10) = 230s. That relationship is
#    asserted in tests/test_deploy_script.py, so raising a `start_period` past
#    this timeout fails CI rather than manufacturing a deploy failure.
#
#    Exempt services come from ../healthcheck-exemptions.txt, the same file
#    TestServiceHealthCoverage reads. "No healthcheck configured" means *not
#    pollable*, never *not healthy*.
#
# 3. Deploy coordination is drained and authorized before mutation, then
#    released on the way out — except after a failed mutation.
#    This is the behaviour the old EXIT trap had by accident, split
#    into the two cases it was conflating:
#
#      - Nothing was recreated (bad arguments, a failed build): release. No
#        container changed, so blocking DAGs serves no purpose.
#      - A container was recreated or restarted and something then failed:
#        HOLD. The fleet may be half-deployed, and resuming work against a
#        mixed fleet is worse than a stalled pipeline. Held coordination is
#        independently alertable and the indefinite Airflow gate does not turn
#        the pause into failed DAGs. Release it by hand only after entering
#        validation once the fleet is sane (commands printed by the failure
#        path).
#
#    Either way a Telegram alert fires on failure, naming the phase and
#    whether intent was released or held.
#
# 4. `--restart` for bind-mounted config files. Six services mount a single
#    *file* rather than a directory (prometheus.yml, promtail.yml, loki.yml,
#    statsd_mapping.yml, Caddyfile, oauth2-proxy.cfg). A single-file bind
#    mount pins the inode; `git pull` replaces the file rather than editing it
#    in place, so the container keeps reading the old, now-unlinked one. A
#    SIGHUP reload is worse than useless there — it logs "Completed loading of
#    configuration file" against the stale config, which is how this went
#    unnoticed twice on 2026-08-20. `docker compose restart` re-resolves bind
#    mounts, and this mode then *verifies* it by comparing host and container
#    inodes rather than trusting the restart.
#
# 5. A recreate warns about peers that cache its address. Same family as 4 —
#    a deploy action with an invisible side effect on a service nobody named.
#    Recreating a container changes its IP; a long-lived process that resolved
#    the old address once keeps using it, and over UDP that fails with no
#    exception, no log and no error metric while both containers stay healthy.
#    The Plan 140 Stage 1 deploy recreated `statsd-exporter` and cost two days
#    of Airflow metrics that way (Plan 136 D6). ../deploy-followers.txt names
#    the services this applies to and what to restart. The script prints the
#    note and does not act on it: bouncing a service the operator did not name
#    is the same defect `--no-deps` exists to stop.
#
#    Only recreates are exposed. `--restart` keeps the container and
#    its address, and TCP peers (`promtail`→`loki`, `postgres-exporter`→
#    `postgres`, every Prometheus scrape) see a connection error, re-resolve
#    and recover — which is why they are not in that file. UDP is the whole
#    hazard, and `up` cannot see it: it was 1 for the entire two days.
#
# 6. A recreate that recreated nothing says so. Found 2026-08-20, immediately
#    after the above shipped, while looking for the right way to restart the
#    three Airflow processes still holding the dead `statsd-exporter` address.
#    `up -d --no-deps` on an unchanged service leaves the container running and
#    exits 0 — correct, and indistinguishable from a real deploy in the output.
#    That is defect 4's shape again: success reported for an action that did
#    nothing. Container ids are sampled before `up -d` and compared after.
#
# ---------------------------------------------------------------------------
# Plan 158 — the seventh decision, added 2026-08-30 after a deploy hung.
#
# 7. The authorize wait is bounded, like every other wait here. Decision 2
#    already states the principle — a health gate, not `sleep 10`, and a
#    recreated service that never goes healthy fails the deploy loudly — and
#    the drain poll was the one wait that did not follow it. On 2026-08-30 a
#    `redeploy.sh ops processing` sat in `while :` for nineteen minutes with
#    every production DAG parked, and would have sat there indefinitely; it
#    ended because an operator pressed Ctrl-C. The operator was the timeout.
#
#    On expiry the script prints the drain evidence, names the blocking
#    sources, and returns 1. Nothing has been recreated at that point —
#    `_prepare_coordination` runs before every mutation — so `MUTATED` is 0
#    and the trap in decision 3 releases intent and the fleet resumes. A
#    bounded wait therefore converts an unbounded silent hang into a failed
#    deploy with a diagnosis attached, and costs nothing else.
#
#    This is defence in depth, not a fix for any particular blocker. The
#    blocker that motivated it — a gate observation the sensor could never
#    write — is fixed in airflow/dags/sensors.py; this bound is what catches
#    the *next* stuck source, which on the same day was a Plan 145 one-shot
#    container that had outlived its work by fourteen hours.
#
#    `scripts/host_maintenance.py`'s `wait_until_active` polls the same drain
#    and is deliberately unbounded. That is not an inconsistency: a
#    maintenance window has variable package and reboot duration and an
#    operator watching it, whereas deploy intent protects "a short service
#    replacement — expected duration under ten minutes" (Plan 142 D9).
#    DEPLOY_DRAIN_TIMEOUT's default is that stated expectation, and it is
#    generous against the only floor that is checked in: a drain must survive
#    one complete fire-and-park cycle of the tightest-scheduled gated DAG
#    (`*/5`) plus the deploy-intent sensor's 60s poke interval, because a run
#    parked on the gate records its observation only on its next poke. That
#    relationship is asserted in tests/test_deploy_script.py, so tightening a
#    DAG schedule or slowing the sensor fails CI rather than manufacturing a
#    deploy failure.
# ---------------------------------------------------------------------------

set -e
set -o pipefail

OPS_URL="http://localhost:8060"
TELEGRAM_CHAT_ID="774819707"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXEMPT_FILE="$(dirname "$SCRIPT_DIR")/healthcheck-exemptions.txt"
FOLLOWERS_FILE="$(dirname "$SCRIPT_DIR")/deploy-followers.txt"

# See decision 2 above before changing these.
HEALTH_TIMEOUT="${DEPLOY_HEALTH_TIMEOUT:-300}"
HEALTH_POLL_INTERVAL="${DEPLOY_HEALTH_POLL_INTERVAL:-5}"
# See decision 7 above before changing these.
DRAIN_TIMEOUT="${DEPLOY_DRAIN_TIMEOUT:-600}"
DRAIN_POLL_INTERVAL="${DEPLOY_DRAIN_POLL_INTERVAL:-5}"

MODE="recreate"
PHASE="startup"
MUTATED=0          # 1 once a container has been recreated or restarted
UNVERIFIED=0       # single-file mounts --restart could not check (no stat in image)
SERVICES=""
COORDINATION_REQUESTED=0
declare -A BEFORE_ID   # service -> container id, sampled before `up -d`

# Decision 7. Everything the operator needed at 06:15 on 2026-08-30 and had to
# assemble by hand: which sources are holding the drain, and their counts.
_dump_drain_evidence() {
    local evidence
    echo
    echo "--- coordination drain evidence (${OPS_URL}/coordination/drain-status) ---"
    evidence="$(curl -sS "$OPS_URL/coordination/drain-status" 2>/dev/null)" || evidence=""
    if [ -z "$evidence" ]; then
        echo "  drain-status did not answer. The ops API is itself unreachable,"
        echo "  which is very likely why authorization never arrived."
        return 0
    fi
    printf '%s' "$evidence" | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    doc = json.loads(raw)
except ValueError:
    print(raw)
    raise SystemExit(0)
blockers = doc.get("blockers") or []
print("Blocking sources: " + (", ".join(blockers) if blockers else "none reported"))
if not blockers:
    print("  The drain reports no blocker, so a failing authorization is an ops")
    print("  API fault rather than in-scope work. Read /coordination/status.")
for item in doc.get("sources") or []:
    if item.get("source") not in blockers:
        continue
    print("  {}: status={} count={} oldest_started_at={}{}".format(
        item.get("source"), item.get("status"), item.get("count"),
        item.get("oldest_started_at"),
        " reason=" + item["reason"] if item.get("reason") else ""))
print()
print(json.dumps(doc, indent=2, sort_keys=True))
' || printf '%s\n' "$evidence"
}

_prepare_coordination() {
    local status payload start=$SECONDS
    PHASE="drain"
    payload="$(python3 -c \
        'import json,sys; print(json.dumps({"targets": sys.argv[1:]}))' "$@")"
    echo "Requesting deploy coordination for: $SERVICES"
    curl -sf -X POST "$OPS_URL/deploy/start" \
        -H 'Content-Type: application/json' -d "$payload" >/dev/null
    COORDINATION_REQUESTED=1
    echo "Beginning scoped coordination drain (waiting up to ${DRAIN_TIMEOUT}s)..."
    curl -sf -X POST "$OPS_URL/coordination/begin-drain" >/dev/null
    while :; do
        status="$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
            "$OPS_URL/coordination/authorize")"
        case "$status" in
            200)
                echo "Drain confirmed after $(( SECONDS - start ))s; deploy mutation authorized."
                return 0
                ;;
            409)
                if [ $(( SECONDS - start )) -ge "$DRAIN_TIMEOUT" ]; then
                    echo "ERROR: in-scope work did not drain within ${DRAIN_TIMEOUT}s." >&2
                    _dump_drain_evidence
                    echo "Nothing has been recreated, so deploy intent is released on exit" >&2
                    echo "and the fleet resumes by itself. Re-run once the source above is" >&2
                    echo "clear, or raise DEPLOY_DRAIN_TIMEOUT if the wait was legitimate." >&2
                    return 1
                fi
                echo "  In-scope work is still draining ($(( SECONDS - start ))s of ${DRAIN_TIMEOUT}s); retrying in ${DRAIN_POLL_INTERVAL}s."
                sleep "$DRAIN_POLL_INTERVAL"
                ;;
            *)
                echo "ERROR: coordination authorization returned HTTP ${status}." >&2
                return 1
                ;;
        esac
    done
}

_begin_validation() {
    PHASE="begin-validation"
    echo "Recording successful deploy health checks; beginning validation..."
    curl -sf -X POST "$OPS_URL/coordination/begin-validation" >/dev/null
}

_on_exit() {
    local exit_code=$?
    local intent_state

    if [ "$COORDINATION_REQUESTED" -eq 0 ]; then
        intent_state="not requested"
    elif [ "$exit_code" -eq 0 ] || [ "$MUTATED" -eq 0 ]; then
        intent_state="released"
        echo "Signalling deploy complete..."
        curl -sf -X POST "$OPS_URL/deploy/complete" \
            || echo "Warning: failed to signal /deploy/complete"
    else
        intent_state="HELD"
        echo
        echo "Deploy intent HELD: failed during '${PHASE}' after containers were changed."
        echo "  The fleet may be partially deployed, so work is deliberately left paused."
        echo "  Inspect it, then release by hand:"
        echo "    curl -X POST ${OPS_URL}/coordination/begin-validation"
        echo "    curl -X POST ${OPS_URL}/deploy/complete"
    fi

    if [ "$exit_code" -ne 0 ] && [ -n "$TELEGRAM_API" ]; then
        curl -sf -X POST "https://api.telegram.org/bot${TELEGRAM_API}/sendMessage" \
            --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
            --data-urlencode "text=Deploy FAILED [${SERVICES}] during ${PHASE} — intent ${intent_state}. Exit code: ${exit_code}" \
            || echo "Warning: failed to send Telegram alert"
    fi
}
trap _on_exit EXIT

_usage() {
    echo "Usage: $0 [--restart] <service> [service ...]"
    echo "  (default)  new code: build the images, recreate the containers, wait"
    echo "             for health. Does nothing when Compose sees no drift."
    echo "  --restart  same image and config, but the process must restart: a"
    echo "             bind-mounted config file, or a cached peer address."
    echo "             Waits for health, then verifies the files it reads."
    echo "             (--config is an accepted spelling of the same mode.)"
    echo "Example: $0 scraper dbt_runner"
    echo "Example: $0 --restart prometheus"
    echo "Example: $0 --restart airflow-dag-processor airflow-triggerer"
}

# --- exemptions -------------------------------------------------------------

# Service names from healthcheck-exemptions.txt. Comments start with '#' in
# column 0, indented lines continue the previous reason; see that file's header.
_exempt_services() {
    local line
    while IFS= read -r line; do
        case "$line" in
            ''|'#'*|' '*|$'\t'*) continue ;;
        esac
        printf '%s\n' "${line%%[[:space:]]*}"
    done < "$EXEMPT_FILE"
}

_is_exempt() {
    case " $EXEMPT " in
        *" $1 "*) return 0 ;;
    esac
    return 1
}

# --- peers that cache a recreated service's address -------------------------

# Prints deploy-followers.txt's entry for $1 verbatim, or returns 1 if the
# service has no entry. Same file shape as the exemptions file.
_follower_note() {
    local svc="$1" line inside=0 status=1
    [ -f "$FOLLOWERS_FILE" ] || return 1
    while IFS= read -r line; do
        case "$line" in
            '#'*) continue ;;
            '') inside=0; continue ;;
            ' '*|$'\t'*)
                [ "$inside" -eq 1 ] && printf '  %s\n' "$line"
                continue
                ;;
        esac
        if [ "${line%%[[:space:]]*}" = "$svc" ]; then
            inside=1
            status=0
            printf '  %s\n' "$line"
        else
            inside=0
        fi
    done < "$FOLLOWERS_FILE"
    return "$status"
}

_print_follower_notes() {
    local svc note all=""
    for svc in "$@"; do
        note="$(_follower_note "$svc")" || continue
        all="${all}${note}"$'\n'
    done
    [ -z "$all" ] && return 0

    echo
    echo "FOLLOW-UP REQUIRED — a recreated container has a new address, and the peers"
    echo "below cached the old one. They will fail silently, so this script tells you"
    echo "rather than restarting a service you did not name:"
    echo
    printf '%s' "$all"
}

# --- did the recreate actually recreate anything? ---------------------------

# `up -d` is a no-op when Compose sees no drift: same image, same service
# config, container left running. That is the right behaviour and the wrong
# report — the old script would print "Done." and the operator would believe a
# restart had happened. Proved by dry-run against production on 2026-08-20
# while looking for somewhere to re-resolve a peer address; all three services
# came back "Running" and nothing would have changed.
_warn_if_unchanged() {
    local svc cid
    local unchanged=()

    for svc in "$@"; do
        cid="$(_container_id "$svc")"
        if [ -n "${BEFORE_ID[$svc]}" ] && [ "${BEFORE_ID[$svc]}" = "$cid" ]; then
            unchanged+=("$svc")
        fi
    done
    [ ${#unchanged[@]} -eq 0 ] && return 0

    echo
    echo "NOTE: ${unchanged[*]} kept the same container. Compose found no change to"
    echo "      apply, so nothing was recreated and no new code is running. If you"
    echo "      expected new code, the build produced no new image. To re-resolve a"
    echo "      peer address or pick up a bind-mounted file, use --restart."
    echo
}

# --- health gate ------------------------------------------------------------

_container_id() {
    docker compose ps -q "$1" 2>/dev/null | head -n 1 || true
}

# Echoes "<container status> <health status>"; health is "none" when the image
# has no healthcheck, which is not the same thing as unhealthy.
_state_of() {
    docker inspect \
        --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
        "$1" 2>/dev/null || echo "missing none"
}

_dump_failure() {
    local svc cid
    echo
    echo "--- fleet state (dependencies are checked here, never recreated) ---"
    docker compose ps || true
    for svc in "$@"; do
        cid="$(_container_id "$svc")"
        [ -z "$cid" ] && continue
        echo "--- ${svc} healthcheck log ---"
        docker inspect \
            --format '{{range .State.Health.Log}}exit={{.ExitCode}} {{.Output}}{{end}}' \
            "$cid" 2>/dev/null || true
    done
}

_wait_for_health() {
    local start=$SECONDS
    local svc cid status health
    local pending=()

    for svc in "$@"; do
        if _is_exempt "$svc"; then
            echo "  ${svc}: exempt from health polling (healthcheck-exemptions.txt)"
            continue
        fi
        cid="$(_container_id "$svc")"
        if [ -z "$cid" ]; then
            echo "ERROR: ${svc} has no container after deploy." >&2
            _dump_failure "$@"
            return 1
        fi
        read -r status health <<< "$(_state_of "$cid")"
        if [ "$health" = "none" ]; then
            echo "  WARNING: ${svc} has no healthcheck and is not exempt, so it cannot be"
            echo "           polled. TestServiceHealthCoverage should have caught this."
            continue
        fi
        pending+=("$svc")
    done

    if [ ${#pending[@]} -eq 0 ]; then
        echo "  Nothing pollable in this deploy; not waiting."
        return 0
    fi

    echo "Waiting up to ${HEALTH_TIMEOUT}s for health: ${pending[*]}"

    local last_report="" next_beat=$SECONDS
    while :; do
        local still=() report=""
        for svc in "${pending[@]}"; do
            cid="$(_container_id "$svc")"
            read -r status health <<< "$(_state_of "$cid")"
            case "$status" in
                exited|dead|missing)
                    echo "ERROR: ${svc} is ${status}; it will not become healthy." >&2
                    _dump_failure "${pending[@]}"
                    return 1
                    ;;
            esac
            if [ "$health" != "healthy" ]; then
                still+=("$svc")
                report="${report} ${svc}=${health}"
            fi
        done

        if [ ${#still[@]} -eq 0 ]; then
            echo "  All pollable services healthy after $(( SECONDS - start ))s."
            return 0
        fi

        if [ "$report" != "$last_report" ] || [ "$SECONDS" -ge "$next_beat" ]; then
            echo "  [$(( SECONDS - start ))s]${report}"
            last_report="$report"
            next_beat=$(( SECONDS + 30 ))
        fi

        if [ $(( SECONDS - start )) -ge "$HEALTH_TIMEOUT" ]; then
            echo "ERROR: timed out after ${HEALTH_TIMEOUT}s waiting for:${report}" >&2
            _dump_failure "${still[@]}"
            return 1
        fi

        pending=("${still[@]}")
        sleep "$HEALTH_POLL_INTERVAL"
    done
}

# --- bind-mounted config verification ---------------------------------------

# A single-file bind mount pins the inode it was resolved to at container
# start. Equal inodes mean the container is reading the file that is on disk
# now; different inodes mean it is reading a deleted one (decision 4).
_verify_config_mounts() {
    local svc cid src dst host_inode container_inode
    local checked=0 problems=0

    for svc in "$@"; do
        cid="$(_container_id "$svc")"
        [ -z "$cid" ] && continue
        while IFS='|' read -r src dst; do
            [ -z "$src" ] && continue
            # Directory mounts are immune: names resolve on every access.
            [ -f "$src" ] || continue
            checked=$(( checked + 1 ))
            host_inode="$(stat -c %i "$src")"
            if ! container_inode="$(docker exec "$cid" stat -c %i "$dst" 2>/dev/null)"; then
                echo "  UNVERIFIED ${svc}:${dst} — no usable stat in the image (distroless?)."
                UNVERIFIED=$(( UNVERIFIED + 1 ))
                continue
            fi
            if [ "$host_inode" = "$container_inode" ]; then
                echo "  OK ${svc}:${dst} — inode ${host_inode} matches the file on disk."
            else
                echo "  STALE ${svc}:${dst} — container reads inode ${container_inode}," >&2
                echo "        the file on disk is ${host_inode}. The restart did not take." >&2
                problems=$(( problems + 1 ))
            fi
        done <<< "$(docker inspect \
            --format '{{range .Mounts}}{{if eq .Type "bind"}}{{.Source}}|{{.Destination}}{{println}}{{end}}{{end}}' \
            "$cid")"
    done

    if [ "$checked" -eq 0 ]; then
        echo "  No single-file bind mounts on these services; nothing to verify."
    fi
    [ "$problems" -eq 0 ]
}

# --- main -------------------------------------------------------------------

case "$1" in
    --restart|--config)
        # One mode, two honest names. The mechanism is restart-and-verify;
        # `--config` is kept because deploying a bind-mounted config file is
        # the most common reason to reach for it, and it reads better at the
        # call site than the mechanism does.
        MODE="restart"
        shift
        ;;
esac

if [ $# -eq 0 ]; then
    _usage
    exit 1
fi

SERVICES="$*"

if [ ! -f "$EXEMPT_FILE" ]; then
    echo "ERROR: ${EXEMPT_FILE} is missing, so exempt services cannot be told" >&2
    echo "       apart from unhealthy ones. Refusing to deploy." >&2
    exit 1
fi
EXEMPT="$(_exempt_services | tr '\n' ' ')"

if [ "$MODE" = "restart" ]; then
    _prepare_coordination "$@"
    PHASE="restart"
    MUTATED=1
    echo "Restarting in place: $SERVICES"
    docker compose restart "$@"

    PHASE="health"
    _wait_for_health "$@"

    PHASE="verify"
    echo "Verifying bind-mounted config files..."
    _verify_config_mounts "$@"

    _begin_validation

    PHASE="done"
    if [ "$UNVERIFIED" -gt 0 ]; then
        echo "Done — restarted and healthy, but ${UNVERIFIED} mount(s) could not"
        echo "       be verified; check them by hand (see above)."
    else
        echo "Done — restarted, containers healthy, mounts verified current."
    fi
else
    PHASE="build"
    echo "Building: $SERVICES"
    docker compose build "$@"

    _prepare_coordination "$@"
    PHASE="recreate"
    MUTATED=1
    for svc in "$@"; do
        BEFORE_ID[$svc]="$(_container_id "$svc")"
    done
    echo "Recreating (no dependencies): $SERVICES"
    docker compose up -d --no-deps "$@"
    _warn_if_unchanged "$@"

    PHASE="health"
    _wait_for_health "$@"

    _begin_validation

    PHASE="done"
    echo "Done — every pollable service reported healthy."
    _print_follower_notes "$@"
fi
