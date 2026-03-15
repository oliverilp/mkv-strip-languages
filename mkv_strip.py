#!/usr/bin/env python3
"""
mkv_strip.py — Remove unwanted language tracks from MKV files.

Strips audio and subtitle tracks for specified languages (default: Russian + Ukrainian),
keeps everything else, sets the first remaining audio track as default.
No re-encoding — pure remux, fast and lossless.

Safety guarantees:
  - Original file is NEVER touched until output is fully written and verified
  - If ffmpeg fails or output is suspiciously small, output is deleted, original kept
  - If removing tracks would leave NO audio, the file is skipped entirely
  - Any unexpected exception aborts cleanly without touching the original
  - Dry-run mode available to preview changes before applying

Usage:
    python mkv_strip.py movie.mkv --dry-run          # preview only
    python mkv_strip.py movie.mkv                     # write movie.stripped.mkv
    python mkv_strip.py movie.mkv --in-place          # replace original (atomic rename)
    python mkv_strip.py /path/to/movies/              # batch process directory
    python mkv_strip.py movie.mkv --remove-langs rus ukr deu  # custom languages

Dependencies: ffmpeg + ffprobe (brew install ffmpeg)
"""

import argparse
import json
import shutil
import subprocess
import sys
import traceback
from pathlib import Path

DEFAULT_REMOVE_LANGS = {"rus", "ukr"}

# Fallback: match these substrings in track title (case-insensitive, Cyrillic included)
TITLE_KEYWORDS = {"russian", "ukrainian", "украин", "русск", "рос"}

# If output is smaller than this fraction of the input, something is likely wrong
MIN_OUTPUT_SIZE_RATIO = 0.05


def run(cmd: list[str], capture: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
    )


def probe(path: Path) -> list[dict] | None:
    """
    Run ffprobe on path and return list of stream dicts.
    Returns None on any failure (bad file, unreadable, not an MKV, etc).
    """
    try:
        result = run([
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            str(path),
        ])
    except Exception as e:
        print(f"  ERROR: Could not run ffprobe: {e}")
        return None

    if result.returncode != 0:
        print(f"  ERROR: ffprobe exited with code {result.returncode}")
        if result.stderr:
            print(f"         {result.stderr.strip()}")
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"  ERROR: ffprobe returned invalid JSON: {e}")
        return None

    streams = data.get("streams")
    if not isinstance(streams, list):
        print(f"  ERROR: ffprobe output had no 'streams' list")
        return None

    return streams


def should_remove(stream: dict, remove_langs: set[str]) -> bool:
    """Return True if this audio/subtitle stream should be stripped."""
    codec_type = stream.get("codec_type", "")
    if codec_type not in ("audio", "subtitle"):
        return False

    tags = stream.get("tags") or {}
    lang = tags.get("language", "").lower().strip()
    title = tags.get("title", "").lower().strip()

    if lang in remove_langs:
        return True

    # Fallback: title keyword match (handles mislabelled/untagged tracks)
    if any(kw in title for kw in TITLE_KEYWORDS):
        return True

    return False


def format_stream(stream: dict) -> str:
    idx = stream.get("index", "?")
    codec_type = stream.get("codec_type", "?")
    codec = stream.get("codec_name", "?")
    tags = stream.get("tags") or {}
    lang = tags.get("language", "und")
    title = tags.get("title", "")
    channels = stream.get("channels", "")
    ch_str = f" {channels}ch" if channels else ""
    title_str = f' "{title}"' if title else ""
    return f"[{idx:2d}] {codec_type:<9} {codec:<8} lang={lang:<4}{ch_str}{title_str}"


def cleanup(path: Path) -> None:
    """Delete a file if it exists, silently."""
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def process_file(
    path: Path,
    remove_langs: set[str],
    dry_run: bool,
    in_place: bool,
) -> bool:
    """
    Analyse and optionally remux one MKV file.
    Returns True if the file was (or would be) modified.
    The original file is never modified or deleted unless ffmpeg fully succeeds.
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}► {path.name}")

    if not path.exists():
        print(f"  ERROR: File not found.")
        return False

    if not path.is_file():
        print(f"  ERROR: Not a regular file.")
        return False

    streams = probe(path)

    if streams is None:
        print(f"  SKIPPING — ffprobe failed. File untouched.")
        return False

    if not streams:
        print(f"  SKIPPING — no streams found. File untouched.")
        return False

    keep = []
    remove = []

    for s in streams:
        if should_remove(s, remove_langs):
            print(f"  ✗ REMOVE  {format_stream(s)}")
            remove.append(s)
        else:
            print(f"  ✓ KEEP    {format_stream(s)}")
            keep.append(s)

    if not remove:
        print("  → No matching tracks found. File untouched.")
        return False

    # Safety: must keep at least one audio track
    kept_audio = [s for s in keep if s.get("codec_type") == "audio"]
    if not kept_audio:
        print("  ⚠ SKIPPING — removing these tracks would leave NO audio. File untouched.")
        return False

    # Safety: must keep at least one real video track (exclude attached_pic cover art)
    kept_video = [
        s for s in keep
        if s.get("codec_type") == "video"
        and not s.get("disposition", {}).get("attached_pic", 0)
    ]
    if not kept_video:
        print("  ⚠ SKIPPING — no video track would remain. File untouched.")
        return False

    removed_types = sorted({s.get("codec_type") for s in remove})
    print(f"  → Will remove {len(remove)} track(s) ({', '.join(removed_types)})")

    if dry_run:
        print(f"  → Dry run complete. Run without --dry-run to apply.")
        return True

    # --- Build ffmpeg command ---
    remove_indices = {s["index"] for s in remove}

    # Map all streams, then negate the ones to remove by stream index
    map_args = ["-map", "0"]
    for idx in sorted(remove_indices):
        map_args += ["-map", f"-0:{idx}"]

    # Set default flag on first kept audio, clear it on all others
    disposition_args = []
    audio_out_idx = 0
    for s in keep:
        if s.get("codec_type") == "audio":
            flag = "default" if audio_out_idx == 0 else "0"
            disposition_args += [f"-disposition:a:{audio_out_idx}", flag]
            audio_out_idx += 1

    out_path = path.with_stem(path.stem + ".stripped")

    # Remove any leftover .stripped file from a previous failed run
    if out_path.exists():
        print(f"  → Removing leftover: {out_path.name}")
        out_path.unlink()

    cmd = [
        "ffmpeg", "-y",
        "-i", str(path),
        *map_args,
        "-c", "copy",
        *disposition_args,
        str(out_path),
    ]

    print(f"  → Running ffmpeg...")
    print(f"     {' '.join(cmd)}\n")

    try:
        result = run(cmd, capture=False)
    except Exception as e:
        print(f"\n  ERROR: Failed to launch ffmpeg: {e}")
        cleanup(out_path)
        return False

    if result.returncode != 0:
        print(f"\n  ERROR: ffmpeg exited with code {result.returncode}. Original untouched.")
        cleanup(out_path)
        return False

    # Verify output
    if not out_path.exists():
        print(f"  ERROR: ffmpeg succeeded but output file missing. Original untouched.")
        return False

    orig_size = path.stat().st_size
    new_size = out_path.stat().st_size

    if new_size == 0:
        print(f"  ERROR: Output is 0 bytes. Deleting it. Original untouched.")
        cleanup(out_path)
        return False

    if new_size < orig_size * MIN_OUTPUT_SIZE_RATIO:
        print(
            f"  ERROR: Output ({new_size // 1_048_576} MB) is suspiciously small vs "
            f"original ({orig_size // 1_048_576} MB). Deleting output. Original untouched."
        )
        cleanup(out_path)
        return False

    saved_mb = (orig_size - new_size) / 1_048_576
    print(f"  ✓ Success: {orig_size // 1_048_576} MB → {new_size // 1_048_576} MB  (saved {saved_mb:.1f} MB)")

    if in_place:
        try:
            out_path.replace(path)  # atomic on same filesystem
            print(f"  → Replaced original: {path.name}")
        except Exception as e:
            print(f"  ERROR: Could not replace original: {e}")
            print(f"  → Stripped file kept at: {out_path.name}")
    else:
        print(f"  → Output written: {out_path.name}")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Strip Russian/Ukrainian (or custom language) tracks from MKV files.",
    )
    parser.add_argument("input", help="MKV file or directory of MKV files")
    parser.add_argument(
        "--remove-langs", nargs="+", default=list(DEFAULT_REMOVE_LANGS),
        metavar="LANG",
        help=f"ISO 639-2 language codes to remove (default: {' '.join(sorted(DEFAULT_REMOVE_LANGS))})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would happen without making any changes",
    )
    parser.add_argument(
        "--in-place", action="store_true",
        help="Replace original file after successful remux (uses atomic rename — safe)",
    )
    args = parser.parse_args()

    for tool in ("ffmpeg", "ffprobe"):
        if shutil.which(tool) is None:
            print(f"ERROR: '{tool}' not found. Install with: brew install ffmpeg")
            sys.exit(1)

    remove_langs = {lang.lower() for lang in args.remove_langs}
    print(f"Languages to remove: {', '.join(sorted(remove_langs))}")
    if args.dry_run:
        print("DRY RUN — no files will be created or modified.\n")

    input_path = Path(args.input)

    if input_path.is_file():
        if input_path.suffix.lower() != ".mkv":
            print(f"WARNING: '{input_path.name}' has no .mkv extension. Proceeding anyway.")
        files = [input_path]
    elif input_path.is_dir():
        files = sorted(input_path.glob("*.mkv"))
        if not files:
            print(f"No .mkv files found in: {input_path}")
            sys.exit(0)
        print(f"Found {len(files)} MKV file(s) in {input_path}")
    else:
        print(f"ERROR: '{input_path}' does not exist or is not a file/directory.")
        sys.exit(1)

    modified = skipped = errors = 0

    for f in files:
        try:
            if process_file(f, remove_langs, args.dry_run, args.in_place):
                modified += 1
            else:
                skipped += 1
        except Exception:
            print(f"\n  UNEXPECTED ERROR processing {f.name}:")
            traceback.print_exc()
            print(f"  File untouched.")
            errors += 1

    print(f"\n{'='*60}")
    print(f"Done.  modified={modified}  skipped={skipped}  errors={errors}  total={len(files)}")


if __name__ == "__main__":
    main()
