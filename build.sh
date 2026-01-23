#!/usr/bin/env bash
set -e

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$SRC_DIR/build"
DLL_DIR="$(cd "$SRC_DIR/.." && pwd)"

mkdir -p "$OUT_DIR"

echo "SRC_DIR=$SRC_DIR"
echo "OUT_DIR=$OUT_DIR"
echo "DLL_DIR=$DLL_DIR"

mcs -langversion:latest \
  -out:"$OUT_DIR/fft_trigger_controller.exe" \
  -r:"$DLL_DIR/VSEUtilities_API.dll" \
  "$SRC_DIR/FFTTriggerController.cs"

echo "OK: $OUT_DIR/fft_trigger_controller.exe"
ls -lrt "$OUT_DIR/fft_trigger_controller.exe"
