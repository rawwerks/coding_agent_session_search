#!/usr/bin/env python3
"""
Import ChatGPT web export into a format cass can index.

PROBLEM:
The ChatGPT Mac app encrypts conversations (v2/v3) using AES-256-GCM with a key
stored in macOS Keychain under access group '2DC432GLL2.com.openai.shared'.
This key is protected by Apple's code signing - only apps signed with OpenAI's
certificate can access it. Third-party tools cannot decrypt these files.

SOLUTION:
Users can export their data from ChatGPT web (Settings → Data Controls → Export).
This produces a 'conversations.json' with the same JSON structure as decrypted files.

The ChatGPT connector determines encryption by directory name:
  - conversations-v2-{uuid}/ or conversations-v3-{uuid}/ → encrypted, needs key
  - conversations-{anything}/ (no -v2- or -v3-) → unencrypted, directly readable

This script splits the web export into individual files in 'conversations-web-export/',
which cass treats as unencrypted v1 format and indexes without needing the key.

USAGE:
    # Basic usage (auto-detects ChatGPT app support directory)
    python3 import-chatgpt-export.py ~/Downloads/chatgpt-export/conversations.json

    # Specify output directory
    python3 import-chatgpt-export.py conversations.json --output-dir /path/to/output

    # JSON output for automation
    python3 import-chatgpt-export.py conversations.json --json

After importing, run `cass index` to index the conversations.
"""

import argparse
import json
import os
import sys
from pathlib import Path


def get_default_output_dir() -> Path:
    """Get the default ChatGPT app support directory (macOS only)."""
    home = Path.home()
    return home / "Library" / "Application Support" / "com.openai.chat"


def import_chatgpt_export(
    export_path: Path,
    output_dir: Path,
    verbose: bool = False,
) -> dict:
    """
    Import ChatGPT web export into cass-indexable format.

    Args:
        export_path: Path to conversations.json from ChatGPT web export
        output_dir: Base directory (conversations-web-export/ will be created inside)
        verbose: Print progress messages

    Returns:
        Dict with import statistics
    """
    # Create conversations directory (no -v2- or -v3- = unencrypted)
    conv_dir = output_dir / "conversations-web-export"
    conv_dir.mkdir(parents=True, exist_ok=True)

    # Load export
    if verbose:
        print(f"Loading {export_path}...", file=sys.stderr)

    with open(export_path, "r", encoding="utf-8") as f:
        conversations = json.load(f)

    if not isinstance(conversations, list):
        raise ValueError("Expected conversations.json to contain a JSON array")

    total = len(conversations)
    imported = 0
    skipped = 0

    if verbose:
        print(f"Found {total} conversations", file=sys.stderr)

    for i, conv in enumerate(conversations):
        # Extract conversation ID
        conv_id = (
            conv.get("id")
            or conv.get("conversation_id")
            or f"conv-{i}"
        )

        filepath = conv_dir / f"{conv_id}.json"

        # Skip if already exists (idempotent)
        if filepath.exists():
            skipped += 1
            continue

        # Write individual conversation
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(conv, f)

        imported += 1

        if verbose and (imported % 100 == 0):
            print(f"  Processed {imported}/{total}...", file=sys.stderr)

    return {
        "success": True,
        "total": total,
        "imported": imported,
        "skipped": skipped,
        "output_dir": str(conv_dir),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Import ChatGPT web export into cass-indexable format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s ~/Downloads/chatgpt-export/conversations.json
  %(prog)s conversations.json --output-dir ~/custom/path
  %(prog)s conversations.json --json

After importing, run `cass index` to index the conversations.
        """,
    )
    parser.add_argument(
        "export_file",
        type=Path,
        help="Path to conversations.json from ChatGPT web export",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: ~/Library/Application Support/com.openai.chat/)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show progress messages",
    )

    args = parser.parse_args()

    # Validate export file
    if not args.export_file.exists():
        print(f"Error: Export file not found: {args.export_file}", file=sys.stderr)
        sys.exit(1)

    # Determine output directory
    output_dir = args.output_dir or get_default_output_dir()

    try:
        result = import_chatgpt_export(
            args.export_file,
            output_dir,
            verbose=args.verbose or not args.json,
        )

        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"\nImport complete!")
            print(f"  Total conversations: {result['total']}")
            print(f"  Newly imported:      {result['imported']}")
            print(f"  Skipped (existing):  {result['skipped']}")
            print(f"  Output directory:    {result['output_dir']}")
            print(f"\nNext step: Run `cass index` to index the conversations.")

    except Exception as e:
        if args.json:
            print(json.dumps({"success": False, "error": str(e)}))
        else:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
