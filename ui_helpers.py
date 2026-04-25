"""
Shared Streamlit UI helpers used by both app.py and app_backtest.py.
"""

import uuid

import streamlit as st

from jobs import JobStatus, registry


def _get_user_token() -> str:
    if "user_token" not in st.session_state:
        st.session_state["user_token"] = str(uuid.uuid4())
    return st.session_state["user_token"]


def _render_job_progress(job) -> None:
    _icons = {"info": "▸", "warning": "⚠️", "error": "❌", "success": "✅"}
    label = "⏳ Queued…" if job.status.value == "QUEUED" else "⏳ Running in background…"
    with st.container(border=True):
        st.markdown(f"**{label}**")
        for ev in list(job.events):
            st.write(f"{_icons.get(ev['level'], '▸')} {ev['msg']}")


def _poll_job(kind: str, worker, submit_params: dict = None) -> bool:
    """Submit (if triggered) and poll a background job.
    Returns True if the caller should stop rendering (job in progress or errored).
    On success, caches result in st.session_state[f"{kind}_cached_result"].
    """
    user_token = _get_user_token()
    job_key_ss = f"{kind}_job_key"
    cache_ss = f"{kind}_cached_result"

    if st.session_state.pop(f"{kind}_run_triggered", False):
        job = registry.submit(user_token, kind, submit_params or {}, worker)
        st.session_state[job_key_ss] = job.key
        st.session_state.pop(cache_ss, None)

    job = registry.latest(user_token, kind)
    job_key = st.session_state.get(job_key_ss)
    if job is None or job.key != job_key:
        return False

    if job.status in (JobStatus.RUNNING, JobStatus.QUEUED):
        _render_job_progress(job)
        return True

    st.session_state.pop(job_key_ss, None)
    if job.status == JobStatus.DONE:
        st.session_state[cache_ss] = job.result
    elif job.status == JobStatus.ERROR:
        st.error(f"❌ {job.error}")
        return True
    return False
