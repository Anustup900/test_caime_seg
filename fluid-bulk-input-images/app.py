"""
Streamlit dashboard for fluid-bulk segmentation results.

Run from inside fluid-bulk-input-images/:
    streamlit run app.py
"""

import json
from pathlib import Path

import streamlit as st
from PIL import Image

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
OUTPUT_DIR  = BASE_DIR / "fluid-bulk-output"
RESULTS_FILE = OUTPUT_DIR / "results.json"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Segmentation Dashboard",
    page_icon="✂️",
    layout="wide",
)

# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)          # refresh every 30 s so live runs update
def load_results():
    if not RESULTS_FILE.exists():
        return {}
    with open(RESULTS_FILE) as f:
        return json.load(f)

results = load_results()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("✂️ Seg Dashboard")
st.sidebar.markdown("---")

# Stats
total     = len(results)
done      = sum(1 for v in results.values() if v.get("status") == "done")
failed    = sum(1 for v in results.values() if v.get("status") == "error")
missing   = total - done - failed

st.sidebar.metric("✅ Processed",  done)
st.sidebar.metric("❌ Failed",     failed)
st.sidebar.metric("⚠️ Missing src", missing)
st.sidebar.metric("📦 Total",      total)

st.sidebar.markdown("---")

# Class filter
all_classes = sorted({
    v.get("class", "unknown")
    for v in results.values()
    if v.get("status") == "done"
})
selected_classes = st.sidebar.multiselect(
    "Filter by class",
    options=all_classes,
    default=all_classes,
)

# Status filter
show_done    = st.sidebar.checkbox("Show done",    value=True)
show_failed  = st.sidebar.checkbox("Show failed",  value=False)
show_missing = st.sidebar.checkbox("Show missing", value=False)

# View toggle
show_mask = st.sidebar.checkbox("Show mask column", value=False)

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

# ── Main ──────────────────────────────────────────────────────────────────────
st.title("✂️ Segmentation Results")

if not results:
    st.warning("No results yet. Run `python process_images.py` first.")
    st.stop()

# ── Filter rows ───────────────────────────────────────────────────────────────
def keep(info):
    status = info.get("status")
    if status == "done":
        if not show_done:
            return False
        if info.get("class", "unknown") not in selected_classes:
            return False
        return True
    if status == "error":
        return show_failed
    return show_missing   # missing_source or anything else

filtered = {stem: info for stem, info in results.items() if keep(info)}

st.caption(f"Showing **{len(filtered)}** of **{total}** items")

if not filtered:
    st.info("No items match the current filters.")
    st.stop()

# ── Grid ─────────────────────────────────────────────────────────────────────
COLS = 3          # cards per row (orig | segmented [| mask])

def load_img(path: Path):
    """Open image; return None if missing or unreadable."""
    try:
        if path.exists():
            return Image.open(path)
    except Exception:
        pass
    return None

def status_badge(status):
    return {"done": "✅", "error": "❌", "missing_source": "⚠️"}.get(status, "❓")

items = list(filtered.items())

for row_start in range(0, len(items), COLS):
    row_items = items[row_start : row_start + COLS]
    cols = st.columns(COLS)

    for col, (stem, info) in zip(cols, row_items):
        status     = info.get("status", "unknown")
        class_name = info.get("class",  "—")
        source     = info.get("source", "")
        outputs    = info.get("outputs", {})
        timestamp  = info.get("timestamp", "")

        # Resolve file paths
        orig_candidates = list((OUTPUT_DIR / stem).glob("original.*")) if (OUTPUT_DIR / stem).exists() else []
        orig_path   = orig_candidates[0] if orig_candidates else None
        seg_path    = BASE_DIR / outputs["segmented"] if "segmented" in outputs else None
        mask_path   = BASE_DIR / outputs["mask"]      if "mask"      in outputs else None

        with col:
            # Card header
            st.markdown(
                f"**{status_badge(status)} {class_name}**  \n"
                f"<small style='color:grey'>{stem[:28]}…</small>",
                unsafe_allow_html=True,
            )

            if status == "done":
                # Two or three images side by side inside the card
                sub_cols = st.columns([1, 1, 1] if show_mask else [1, 1])
                img_labels = ["Original", "Segmented"] + (["Mask"] if show_mask else [])
                img_paths  = [orig_path,  seg_path]    + ([mask_path] if show_mask else [])

                for scol, label, ipath in zip(sub_cols, img_labels, img_paths):
                    with scol:
                        st.caption(label)
                        img = load_img(ipath) if ipath else None
                        if img:
                            st.image(img, use_container_width=True)
                        else:
                            st.markdown(
                                "<div style='height:100px;display:flex;"
                                "align-items:center;justify-content:center;"
                                "background:#f0f0f0;border-radius:6px;"
                                "color:#999;font-size:12px'>no image</div>",
                                unsafe_allow_html=True,
                            )

                if timestamp:
                    st.caption(f"🕐 {timestamp}")

            elif status == "error":
                st.error(info.get("error", "unknown error"), icon="❌")

            else:   # missing_source
                st.warning(f"Source not found: `{source}`", icon="⚠️")

            st.markdown("---")

# ── Empty columns filler (keeps grid aligned) ─────────────────────────────────
remainder = len(items) % COLS
if remainder:
    for _ in range(COLS - remainder):
        # already zipped — nothing needed; grid pads naturally
        pass
