"""
Bulk ComfyUI image processor.

Reads each image + class name from IMAGE_CLASS_MAP, runs the workflow,
downloads the 'segmented' and 'mask' outputs, and saves everything in
a dashboard-ready folder structure under OUTPUT_DIR.

Output layout:
  fluid-bulk-output/
    results.json                   ← full run log (suitable for a dashboard API)
    {image_stem}/
      segmented.png
      mask.png
"""

import json
import os
import shutil
import time
import traceback
from pathlib import Path

from comfy_api_simplified import ComfyApiWrapper, ComfyWorkflowWrapper

# ── Configuration ────────────────────────────────────────────────────────────
SERVER_ADDRESS = "http://127.0.0.1:8188"   # Change to your ComfyUI server
WORKFLOW_PATH  = "workflow_api.json"        # Path to your exported API-format workflow
OUTPUT_DIR     = Path("fluid-bulk-output")

# Node titles as they appear in your ComfyUI workflow
NODE_IMAGE     = "productImage"            # Load Image node title
NODE_CLASS     = "className"               # String / text node title

# Keywords that appear in the Save-Image filename prefix for each output type.
# ComfyUI names saved files as  <filename_prefix>_00001_.png
# Adjust these if your workflow uses different prefixes.
OUTPUT_TYPES = {
    "segmented": ["segmented"],
    "mask":      ["mask"],
}
# ─────────────────────────────────────────────────────────────────────────────

IMAGE_CLASS_MAP = {
    "fluid-bulk-input-images/0elGG9CoK6U_UDpY1Nfxnuw.jpg": "Jeans",
    "fluid-bulk-input-images/0jCjanM_Bb4_jXWOuvgrV90.png": "shirt",
    "fluid-bulk-input-images/1C92XtKI3qk_UezFbVhZuV4.png": "shirt",
    "fluid-bulk-input-images/1cbqELCozoI_h5bu_idNxcc.jpg": "Jacket",
    "fluid-bulk-input-images/29QLdIAGflA_fbVewD4Ki8s.png": "Jeans",
    "fluid-bulk-input-images/2p0vXwT2gbo_-kgQCH7LF9A.png": "shorts",
    "fluid-bulk-input-images/390ntcWe9u8_BEIbE4toUwA.jpg": "sportsbra",
    "fluid-bulk-input-images/3WOH6x1IFD4_fI2wYcETyQg.png": "bikini",
    "fluid-bulk-input-images/45TRvnbx6ds_IYTjQbWmMg8.png": "Jacket",
    "fluid-bulk-input-images/5ZXVFYhEqX8_T6UJBRnL4Uk.jpg": "tshirt",
    "fluid-bulk-input-images/6F62Q0Ml-Hg_lPGYQDpP7aA.png": "shirt",
    "fluid-bulk-input-images/6nqmU2nc_Jg_zu8-s592jB0.jpg": "suit",
    "fluid-bulk-input-images/6sbBP4QqZ-0_u-DBr1MDQYA.jpg": "jumpsuit",
    "fluid-bulk-input-images/6v27C--G-_s_jdBZYAfAfoU.jpg": "Jeans",
    "fluid-bulk-input-images/7OWl5K3MvNA_zQewM-A1Eh8.jpg": "pouch",
    "fluid-bulk-input-images/7lr604utW0E_CyTQVNlQ2vU.jpg": "tshirt",
    "fluid-bulk-input-images/7xoPmCP_1Sk_wO903TUdBZw.jpeg": "Jeans",
    "fluid-bulk-input-images/8P27fIEVnEs_YjLby6hcEcM.jpg": "Jacket",
    "fluid-bulk-input-images/8eCTJe9SBQU_IAKwZKXQXGc.png": "shorts",
    "fluid-bulk-input-images/9FLCoOuFUmY_4i65ULI9S-U.jpeg": "dress",
    "fluid-bulk-input-images/9YDYIXksK4Q_wps80eaYN_Q.png": "shirt",
    "fluid-bulk-input-images/ABUamSHrPe8_1p2WfrJGCuQ.jpg": "suit",
    "fluid-bulk-input-images/AV6Z6IC5ZtE_QsMARNr6AC4.jpg": "blouse",
    "fluid-bulk-input-images/Aa9DRcYfGW8_2jZC8Ml955Q.jpg": "leggings",
    "fluid-bulk-input-images/AhNcB7N5IGE_FpPmFpYAjlY.png": "tshirt",
    "fluid-bulk-input-images/BV4tLPd9Mjk_9M_QzdCNsGY.png": "Jeans",
    "fluid-bulk-input-images/BazFma5S7yk_PurO1j0tSZg.jpg": "tshirt",
    "fluid-bulk-input-images/Bcnql76tX-c_ixsFZ3sDxEY.png": "bra",
    "fluid-bulk-input-images/C89MYNFcpO8_VDItJd2QtJk.png": "shorts",
    "fluid-bulk-input-images/Dp1RJ4AJLmQ_G7GtDQoRW58.png": "bikini",
    "fluid-bulk-input-images/DtX-ZqsJJJk_eOV4Ju1MgEY.jpg": "tshirt",
    "fluid-bulk-input-images/E5YtMOJvscc_HbmRKSWRxUw.jpg": "kimono",
    "fluid-bulk-input-images/E6ccrpdJ3RE_Ig8eQJR56Lo.png": "shorts",
    "fluid-bulk-input-images/EkTWf8RfUgA_SnzL_KjzMoE.jpeg": "hoodie",
    "fluid-bulk-input-images/FMd9j4L3z5k_2resZ-EFKGM.png": "pants",
    "fluid-bulk-input-images/FNaEazmNVrY_TTzVGTMIRKo.jpg": "pouch",
    "fluid-bulk-input-images/GEeSXiAvWRs_piOy4ioGhI4.png": "Pants",
    "fluid-bulk-input-images/Gk2q4J0yuwc_I3PiXJNMlMI.png": "Jeans",
    "fluid-bulk-input-images/GmQim_QIPfg_R5ulEcym6go.png": "top",
    "fluid-bulk-input-images/H3s_JLCRR7Y_-dtlvOpoNyM.jpg": "heels",
    "fluid-bulk-input-images/HwTKtWe5JwA_Ph0yZ9CUWq4.jpg": "bag",
    "fluid-bulk-input-images/Ik4EXyBO2tk_1-9mMvKahxs.jpg": "pants",
    "fluid-bulk-input-images/J3hkCYzWl9E_UBIaKuPGkik.png": "shirt",
    "fluid-bulk-input-images/JAPIiA-B-cs_qngVweF8bdc.jpg": "Jeans",
    "fluid-bulk-input-images/J_SPQ0K5hno_lR-A24Cy3_I.jpg": "Jacket",
    "fluid-bulk-input-images/Kc9MuoyDT28_hDS9vSEBP4k.jpg": "shirt",
    "fluid-bulk-input-images/L8gur7ONJ74_FHiv3uX11TA.jpg": "heels",
    "fluid-bulk-input-images/LX_fysHk-bs_wFAJM8RmLdM.png": "shirt",
    "fluid-bulk-input-images/MQlPlnoWU-s_vnPVaSHtARk.jpeg": "shirt",
    "fluid-bulk-input-images/Mpu69zu79v0_XaePO_Xe9V0.webp": "coat",
    "fluid-bulk-input-images/Nerf4JP8lPY_xWUjuMOXQDc.png": "shorts",
    "fluid-bulk-input-images/P_48SGDUgd4_efNqo__5LxM.webp": "swimsuit",
    "fluid-bulk-input-images/Pd5_MEV-21c_e1bxWb1Rt7s.png": "Jeans",
    "fluid-bulk-input-images/Pdte7v9bXCQ_kMb4HLZLAo0.tif": "blouse",
    "fluid-bulk-input-images/PqzcDXU6uso_Rao1kJ2wviA.jpg": "blazer",
    "fluid-bulk-input-images/Q2UeqIa0XfI_eTV1lMVDviI.webp": "swimsuit",
    "fluid-bulk-input-images/QDiD3gHCISk_knL6LS_AzUU.png": "Jeans",
    "fluid-bulk-input-images/QEcLnGeEAd8_jeqnuvf95Q4.png": "shorts",
    "fluid-bulk-input-images/QRnzVyrLqoM_yaSsLGAEhQA.png": "Pants",
    "fluid-bulk-input-images/QbWyq-Yx524_ZKZyo8_4Ii0.jpg": "bikini",
    "fluid-bulk-input-images/QskKg-XH1Tk_ThE1ObzZsGY.png": "jumpsuit",
    "fluid-bulk-input-images/R-PabkaO5Lw_BZx3quumv3U.webp": "sweatshirt",
    "fluid-bulk-input-images/R2aJN55mP_Y_KpQekO9rTtM.webp": "shorts",
    "fluid-bulk-input-images/REsod429P_I_MrziwzPJCXM.jpg": "shirt",
    "fluid-bulk-input-images/RH9TzSgSQhs_HE3ysIZVmAA.jpg": "bodysuit",
    "fluid-bulk-input-images/ROl_Fq0KVZU_NO6tbs9xUiQ.jpeg": "Pants",
    "fluid-bulk-input-images/R_rE7IQGWNk_Z7gniT-LdBE.jpeg": "shirt",
    "fluid-bulk-input-images/SnOdTy0piVo_NtLPSbPs7L4.jpg": "skirt",
    "fluid-bulk-input-images/Ui3eTTU5fGk_XxNpv7ZtJ6c.png": "pants",
    "fluid-bulk-input-images/UryX6dwX0Jc_f_UwZjogtdE.png": "kaftan",
    "fluid-bulk-input-images/V8S-OW8N9cU_g5rYT8OxC_c.png": "sweatshirt",
    "fluid-bulk-input-images/VFVkdfu5shc_uwEi4_wl2k4.jpg": "bra",
    "fluid-bulk-input-images/VKBeyGwwl7k_29-LiTGmES8.jpg": "Jacket",
    "fluid-bulk-input-images/VumlSz3OkBs_Qni0H1Or_Vg.png": "shirt",
    "fluid-bulk-input-images/VzqpkPn0dmc_cPp3qHMRFP4.png": "leggings",
    "fluid-bulk-input-images/X4fvfsqGXMk_pqm7WTIn30Y.jpg": "Jacket",
    "fluid-bulk-input-images/XZXZHRbPLr0_csIvLOh9hjM.jpeg": "blazer",
    "fluid-bulk-input-images/YWuXAEZTcmw_zonhBpiHmqE.png": "shirt",
    "fluid-bulk-input-images/Ysh2vY3hw-E_pQhW7zHCNQc.webp": "sportsbra",
    "fluid-bulk-input-images/ZCtETHtc6GI_owcr8tRKZ9Y.png": "bikini",
    "fluid-bulk-input-images/ZO1GHYc1j08_PPmFQI-bh2w.png": "bag",
    "fluid-bulk-input-images/ZihQycyMVOI_5dzLz0ntMc0.jpeg": "dress",
    "fluid-bulk-input-images/_3YtknscmQQ_Effl1nqDFt0.png": "shirt",
    "fluid-bulk-input-images/_4CuQPeAOP4_k_fH4hv8PhE.png": "leggings",
    "fluid-bulk-input-images/_EAajHd4IIo_nheqIzRG_WU.jpg": "Jeans",
    "fluid-bulk-input-images/_oyqSbDG-Xk_g-t0-bbu3rY.jpg": "bra",
    "fluid-bulk-input-images/_w9PbtJKpYQ_AQr8Pc4v3ik.png": "blouse",
    "fluid-bulk-input-images/a64mKA9jIfU_UF8HgbLD-0E.jpg": "shirt",
    "fluid-bulk-input-images/a8-9_FCDDoY_ng8je8SDZQE.png": "jacket",
    "fluid-bulk-input-images/areYju4RIp4_HaQBuQ-2rSM.png": "top",
    "fluid-bulk-input-images/bkrOtPhgtoc_pEIwkjCEHeo.jpg": "heels",
    "fluid-bulk-input-images/c-lMdWREeH8_5njVfNxEKXs.jpg": "swimsuit",
    "fluid-bulk-input-images/clGEkYlvPEI_aaunLBGPCuE.png": "shorts",
    "fluid-bulk-input-images/f4KjdV25WU8_UqjXSrigPJ0.png": "top",
    "fluid-bulk-input-images/faEYyBJIQYA_rOE8n80n1Cs.png": "shorts",
    "fluid-bulk-input-images/fgm75rLtNT0_41krosP6550.jpg": "shirt",
    "fluid-bulk-input-images/ftqSly8yq_s_ipoGgtPRy54.jpg": "swimsuit",
    "fluid-bulk-input-images/fvYe-513DhU_IhzUZIdp1z4.png": "leggings",
    "fluid-bulk-input-images/gXzJDxlrjJU_dziby4UA0mQ.png": "underwear",
    "fluid-bulk-input-images/h2AtPopYVRE_kCuZVugLGVM.png": "shirt",
    "fluid-bulk-input-images/hFF1x3DFDOQ_a6zBpuiZB7g.png": "Jeans",
    "fluid-bulk-input-images/hX7BW2a-wiA_LVu-jZTCDjA.jpg": "bikini",
    "fluid-bulk-input-images/haOeaiyTpxY_IX3AqwR53bQ.jpg": "pouch",
    "fluid-bulk-input-images/iEsir8GSgyc_omjFSXtwaz8.png": "skirt",
    "fluid-bulk-input-images/iGFmb1ciqrI_e7iBrYh_Gss.jpg": "bra",
    "fluid-bulk-input-images/jVOM4wMbygc_b_-z1kyVALM.png": "shorts",
    "fluid-bulk-input-images/jvIB8A_hKKE_9ly1Jdydfos.jpg": "shirt",
    "fluid-bulk-input-images/jwx3vwSqh-E_rHweKHAnsIY.jpg": "bikini",
    "fluid-bulk-input-images/k8DHv0CocMc_jIansiXpmBo.png": "bikini",
    "fluid-bulk-input-images/kK25osoctVM_F2X1lvKaZ4E.jpg": "bodysuit",
    "fluid-bulk-input-images/kNwV9ZStB1Q_YzgjfVDLGkc.jpg": "Jeans",
    "fluid-bulk-input-images/kRWTDOTvMMk_RussMxRKgZ8.png": "sweatshirt",
    "fluid-bulk-input-images/kvInR5C5JJ0_etD8-M1BTUc.png": "pants",
    "fluid-bulk-input-images/l_yfeOD3J10_03KQFoqS1e4.jpeg": "Pants",
    "fluid-bulk-input-images/lsia4DN5D_I_we67Dp14idY.jpg": "swimsuit",
    "fluid-bulk-input-images/mFUHQ6UbRo8_8h2hLbva6vc.png": "Jeans",
    "fluid-bulk-input-images/nLQFStdpbkI_zj78LnKei5s.jpg": "jumpsuit",
    "fluid-bulk-input-images/nM7LhT91f7E_qen9B2yto5o.jpg": "shorts",
    "fluid-bulk-input-images/ns_CvRc3fts_5O1y1GK0POU.png": "tshirt",
    "fluid-bulk-input-images/oPVMoZAblYY_9_HL7wonI24.png": "Jeans",
    "fluid-bulk-input-images/olwXYJArSTg_l-8CcZ3rsZ8.png": "Jeans",
    "fluid-bulk-input-images/ot_FyXyiOBY_dGX3jtW7O3o.png": "cardigan",
    "fluid-bulk-input-images/ouD-g6wrvCk_ashNSyIwkKQ.jpeg": "shirt",
    "fluid-bulk-input-images/pKJftPbQ7AM_lkmJczieu-g.jpg": "blouse",
    "fluid-bulk-input-images/pgTRpAukVK4_e93imIH86XU.jpeg": "Jeans",
    "fluid-bulk-input-images/pywsciW2nIg_bwthp886qYs.png": "Jacket",
    "fluid-bulk-input-images/q78SIjyZjO8_qW1c2c1TXBM.png": "bodysuit",
    "fluid-bulk-input-images/qXBUgYcXuOA_K6_9_RjssUo.jpg": "shirt",
    "fluid-bulk-input-images/qnIGOn3ek3o_Cza-Kt0ep5w.jpg": "blazer",
    "fluid-bulk-input-images/qtPOMrq2Oq8_jvUyF-4Y2c4.png": "coat",
    "fluid-bulk-input-images/rp-ZAeTG6OI_TpyNlo-kGog.png": "sportsbra",
    "fluid-bulk-input-images/s60Up10TOtA_la4cWHYgFHg.jpg": "bag",
    "fluid-bulk-input-images/sbZBAJ1Us8I_EyTEz7zXWPU.png": "kimono",
    "fluid-bulk-input-images/tqPxEcmNBWo_8gYUQyL9oVc.jpeg": "Jeans",
    "fluid-bulk-input-images/uCLNlmAhbGI_7mR3E_GJCis.png": "pants",
    "fluid-bulk-input-images/uMzGt9gi6C0_ttcEP9X033w.jpg": "bag",
    "fluid-bulk-input-images/uQWkDb_hbNU_wI9_jsJwK8I.png": "shirt",
    "fluid-bulk-input-images/u_h8hEl0fAI_VthzzXU8q3U.png": "jacket",
    "fluid-bulk-input-images/w3uJl9gBNdc_XgZ6REbbG2Y.jpeg": "skirt",
    "fluid-bulk-input-images/w70FtmLpJR4_biBkXsYccOc.jpg": "tshirt",
    "fluid-bulk-input-images/xJmAYGH2VC4_llVqo5vKbRI.jpg": "pants",
    "fluid-bulk-input-images/xUStyWO44HQ_iFhlNY18uks.jpeg": "bodysuit",
    "fluid-bulk-input-images/xYGu2YIKYPA_1puc3ql8ykI.webp": "bodysuit",
    "fluid-bulk-input-images/xttMF7pHyeI_7iIQtXIx_lI.png": "Pants",
    "fluid-bulk-input-images/y1LpAr2Ffjs_Egq17uHKTc8.jpg": "jumpsuit",
    "fluid-bulk-input-images/yD7NeIdx8jU_5emPYvFv20Y.jpeg": "hoodie",
    "fluid-bulk-input-images/yK_S0oUR56o_YVpOqPJvtzk.png": "skirt",
    "fluid-bulk-input-images/yNjx367YtRg_BM_09JgydcE.jpg": "bikini",
    "fluid-bulk-input-images/zTcFJ4JlpgE_O1GIUyaZKUs.png": "sweatshirt",
}


def classify_outputs(images: dict) -> dict:
    """
    Splits the raw {filename: bytes} dict returned by queue_and_wait_images()
    into {'segmented': bytes, 'mask': bytes} using OUTPUT_TYPES keyword matching.
    Returns only the types that were matched; unmatched files are logged.
    """
    result = {}
    unmatched = []
    for filename, data in images.items():
        fname_lower = filename.lower()
        matched = False
        for output_type, keywords in OUTPUT_TYPES.items():
            if any(kw in fname_lower for kw in keywords):
                result[output_type] = (filename, data)
                matched = True
                break
        if not matched:
            unmatched.append(filename)

    if unmatched:
        print(f"  [warn] unmatched output files (check OUTPUT_TYPES): {unmatched}")

    return result


def run():
    # Resolve paths relative to this script's directory
    base_dir   = Path(__file__).parent
    output_dir = base_dir / OUTPUT_DIR
    output_dir.mkdir(exist_ok=True)

    # Load existing results so we can resume interrupted runs
    results_path = output_dir / "results.json"
    if results_path.exists():
        with open(results_path) as f:
            results = json.load(f)
    else:
        results = {}

    api = ComfyApiWrapper(SERVER_ADDRESS)

    total  = len(IMAGE_CLASS_MAP)
    done   = 0
    failed = 0

    for rel_path, class_name in IMAGE_CLASS_MAP.items():
        img_path = base_dir / rel_path          # absolute path to source image
        stem     = Path(rel_path).stem          # e.g. "0elGG9CoK6U_UDpY1Nfxnuw"

        # ── Skip already processed ──────────────────────────────────────────
        if stem in results and results[stem].get("status") == "done":
            done += 1
            print(f"[{done}/{total}] skip (cached) {stem}")
            continue

        if not img_path.exists():
            print(f"[WARN] source image not found, skipping: {img_path}")
            results[stem] = {
                "source": rel_path,
                "class":  class_name,
                "status": "missing_source",
            }
            failed += 1
            continue

        print(f"[{done+1}/{total}] {stem}  class={class_name}")

        try:
            # 1. Load fresh workflow for every image to avoid state leakage
            wf = ComfyWorkflowWrapper(str(base_dir / WORKFLOW_PATH))

            # 2. Upload image to ComfyUI and wire it in
            uploaded = api.upload_image(str(img_path))
            wf.set_node_param(NODE_IMAGE, "image", uploaded)

            # 3. Set class name string
            wf.set_node_param(NODE_CLASS, "strings", class_name)

            # 4. Run workflow and collect all output images
            raw_outputs = api.queue_and_wait_images(wf)

            # 5. Sort outputs by type (segmented / mask)
            typed = classify_outputs(raw_outputs)

            if not typed:
                raise RuntimeError(
                    f"No recognized output files. Got: {list(raw_outputs.keys())}"
                )

            # 6. Save outputs
            item_dir = output_dir / stem
            item_dir.mkdir(exist_ok=True)

            # Keep a copy of the source image for the dashboard
            shutil.copy2(img_path, item_dir / f"original{img_path.suffix}")

            saved_files = {}
            for output_type, (orig_name, data) in typed.items():
                out_path = item_dir / f"{output_type}.png"
                out_path.write_bytes(data)
                saved_files[output_type] = str(out_path.relative_to(base_dir))
                print(f"  saved {output_type} → {out_path.name}")

            # 7. Update results index
            results[stem] = {
                "source":      rel_path,
                "class":       class_name,
                "status":      "done",
                "timestamp":   time.strftime("%Y-%m-%dT%H:%M:%S"),
                "outputs":     saved_files,
                "output_dir":  str((output_dir / stem).relative_to(base_dir)),
            }
            done += 1

        except Exception as e:
            print(f"  [ERROR] {stem}: {e}")
            traceback.print_exc()
            results[stem] = {
                "source":  rel_path,
                "class":   class_name,
                "status":  "error",
                "error":   str(e),
            }
            failed += 1

        # Persist after every image so a crash doesn't lose progress
        with open(results_path, "w") as f:
            json.dump(results, f, indent=2)

    # ── Final summary ────────────────────────────────────────────────────────
    print(f"\nDone. processed={done}  failed={failed}  total={total}")
    print(f"Results index: {results_path}")

    # Write a per-class summary (handy for dashboard filtering)
    by_class: dict = {}
    for stem, info in results.items():
        cls = info.get("class", "unknown")
        by_class.setdefault(cls, []).append(stem)
    with open(output_dir / "by_class.json", "w") as f:
        json.dump(by_class, f, indent=2)


if __name__ == "__main__":
    run()
