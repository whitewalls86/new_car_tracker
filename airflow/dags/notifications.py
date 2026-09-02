"""Telegram failure notifications for this repo's DAGs.

This is one module because it used to be three copies, and the copies are the
reason a single commit silenced every pager in the fleet.

``c92bd97`` (2026-05-08 13:10 UTC) added two lines to ``dbt_build``'s notify
task::

    f"Run:     {ti.dag_run.run_id}",
    f"Date:    {ti.execution_date}",

Airflow 3's task SDK ``RuntimeTaskInstance`` has **neither** attribute — its
fields are ``dag_id``, ``run_id``, ``task_id``, ``state`` and friends, with no
``dag_run`` and no ``execution_date``. So the task began raising
``AttributeError`` on its second line, before it built a single message.
``dbt_build``'s notify last succeeded at 12:03 UTC that same day, an hour
before the commit, and never succeeded again: **268 failures, 0 successes
since.** The block was then copied into ``hourly_analytics_refresh`` (12
failures, 0 successes) and into ``pack_bronze_html``, where it has never fired
and so is still latent.

The lesson is not "do not copy code" but that a pager which fails is
indistinguishable from a pipeline that never breaks. Hence this module, and
hence the two properties the copies never had:

- **A non-200 from Telegram is logged.** A rejected send — an over-length body
  is the easy one — otherwise looks exactly like a delivered one.
- **The body is capped below Telegram's 4096-character limit**, because
  quoting several tasks can clear it and Telegram then rejects the whole
  message.

Found while implementing Plan 134 Stage 1, which needs this pager to work
before it is allowed to make any endpoint fail loudly.
"""
import logging
import os

import requests

logger = logging.getLogger(__name__)

# Telegram rejects a sendMessage body over 4096 characters outright, and a
# rejected message is silence. Quoting four tasks with stderr can reach it.
MAX_MESSAGE_CHARS = 4000

# How much of one task's stderr/stdout tail is worth paging. The rest is in
# the task log, which the page exists to send someone to.
_ERROR_BODY_CHARS = 800


def failed_task_ids(context) -> list:
    """Which sibling tasks are in state ``failed`` on this run.

    ``ti.get_task_states`` is the supported Airflow 3 way to see sibling task
    instances — there is no ``ti.dag_run`` to walk. It is a round trip to the
    execution API, so a failure here must not cost the page: the caller still
    quotes every task it was given, it just cannot say which one broke.
    """
    ti = context["ti"]
    run_id = context["dag_run"].run_id
    try:
        states = ti.get_task_states(dag_id=ti.dag_id, run_ids=[run_id])
    except Exception as e:  # noqa: BLE001 - a lookup failure must not eat the page
        logger.warning("could not read sibling task states for %s: %s", run_id, e)
        return []
    run_states = (states or {}).get(run_id) or {}
    return [
        task_id
        for task_id, state in run_states.items()
        # State arrives as a TaskInstanceState enum or as its string value.
        if str(getattr(state, "value", state)) == "failed" and task_id != "notify"
    ]


def task_detail(task_id: str, result, fallback_reason=None) -> list:
    """The lines saying why *task_id* failed, or none if its result cannot say.

    ``failure_reason`` is what a service's 500 carries (see
    ``archiver/app.py``'s predicates and ``dbt_runner``); ``error`` is what a
    summary carries on its own. The cmd/exit/stderr triple is dbt_runner's
    shape. *fallback_reason* is for a job whose summary says it failed in its
    own vocabulary — pack/verify's ``stopped_for_deploy`` and sampled-member
    counts — and is consulted only when neither standard field is set.
    """
    if not result:
        return []
    lines = []
    reason = result.get("failure_reason") or result.get("error")
    if not reason and fallback_reason is not None:
        reason = fallback_reason(result)
    if reason:
        lines.append(f"{task_id}: {reason}")
    if result.get("cmd"):
        lines.append(f"Command: {result['cmd']}")
    rc = result.get("returncode")
    if rc is not None:
        lines.append(f"Exit:    {rc}")
    failures = result.get("failures") or []
    if failures:
        lines.append(f"{task_id} failures: {str(failures)[:_ERROR_BODY_CHARS]}")
    error_body = result.get("stderr") or result.get("stdout") or ""
    if error_body:
        lines += ["", error_body[-_ERROR_BODY_CHARS:]]
    return lines


def send_failure_alert(context, headline: str, *, task_ids, fallback_reason=None) -> bool:
    """Page Telegram for a failed DAG run, naming the task that actually broke.

    *headline* is the DAG's own first line; *task_ids* are the tasks worth
    quoting, in execution order. Tasks that failed are named in a ``Failed:``
    line and quoted first — naming the component rather than the DAG is the
    defect Plan 140 Stage 4 fixed for the health sensors, and a page saying
    only "hourly analytics refresh FAILED" sends a human to read four task
    logs to find out which one broke.

    Every task in *task_ids* is still quoted if it left a result behind, so a
    run whose detail sits on a task that itself succeeded does not lose it.

    Returns True when a message was accepted by Telegram. The credentials are
    read per call rather than at import, so a DAG file's import order cannot
    decide whether the pager works.
    """
    api = os.environ.get("TELEGRAM_API", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not api or not chat_id:
        logger.warning("TELEGRAM_API/TELEGRAM_CHAT_ID not configured - skipping notification")
        return False

    ti = context["ti"]
    dag_run = context["dag_run"]
    failed = failed_task_ids(context)

    lines = [
        headline,
        f"Run:     {dag_run.run_id}",
        # logical_date is None for an asset- or manually-triggered run; run_after
        # is always set. Neither is ti.execution_date, which does not exist.
        f"Date:    {context.get('logical_date') or dag_run.run_after}",
    ]
    if failed:
        lines.append(f"Failed:  {', '.join(failed)}")

    for task_id in list(failed) + [t for t in task_ids if t not in failed]:
        lines += task_detail(
            task_id, ti.xcom_pull(task_ids=task_id, key="result"), fallback_reason
        )

    text = "\n".join(lines)
    if len(text) > MAX_MESSAGE_CHARS:
        text = text[: MAX_MESSAGE_CHARS - 3] + "..."

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{api}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
    except requests.RequestException:
        logger.warning("Failed to send Telegram notification: %s", headline)
        return False

    if not resp.ok:
        # A rejected send is indistinguishable from a delivered one unless it
        # is logged, and these pagers spent months looking delivered.
        logger.warning(
            "Telegram rejected the failure notification (%s): %s %s",
            headline, resp.status_code, resp.text[:300],
        )
        return False
    return True
