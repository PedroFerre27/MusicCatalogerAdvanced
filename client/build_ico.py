"""
build_ico.py — Genera un file .ico multi-resolution dal PNG sorgente.

Da lanciare PRIMA di `pyinstaller tracklab.spec --clean` ogni
volta che si cambia il PNG sorgente o si vuole rigenerare l'icona.

Windows usa dimensioni diverse a seconda del contesto:
  - 16×16   → taskbar piccola, tray, alt+tab
  - 24×24   → taskbar default su DPI standard
  - 32×32   → titlebar Toplevel, taskbar grande
  - 48×48   → Esplora file con icone medie
  - 64×64   → Esplora file su DPI alti
  - 128×128 → Esplora file con icone grandi
  - 256×256 → Esplora file con icone extra-large

Senza un .ico multi-res, Windows scala da una sola dimensione e
l'icona appare sgranata in taskbar (16/24/32). Il fix è incorporare
TUTTE le dimensioni nel .ico, così Windows usa la più adatta nativa.

USAGE:
    python build_ico.py
    # oppure con PNG custom:
    python build_ico.py --src icons/app/taskbar_active.png \
                         --dst icons/tracklab.ico
"""
from pathlib import Path
import argparse
import sys

try:
    from PIL import Image
except ImportError:
    print("ERRORE: serve Pillow. Installa con:  pip install Pillow")
    sys.exit(1)


# Dimensioni standard Windows. Tutte presenti nel .ico finale.
ICO_SIZES = [(16, 16), (24, 24), (32, 32), (48, 48),
             (64, 64), (128, 128), (256, 256)]


def generate_ico(src_png: Path, dst_ico: Path) -> None:
    if not src_png.exists():
        print(f"ERRORE: PNG sorgente non trovato: {src_png}")
        sys.exit(1)
    print(f"[build_ico] sorgente: {src_png}")

    img = Image.open(src_png)
    # Forza RGBA per preservare la trasparenza
    if img.mode != "RGBA":
        img = img.convert("RGBA")

    print(f"[build_ico] dimensione originale: {img.size}")
    if img.size[0] < 256 or img.size[1] < 256:
        print(f"[build_ico] WARNING: PNG sorgente < 256×256, qualità "
              f"finale ridotta. Idealmente usa un PNG 256×256 o più grande.")

    # Per ottenere icona nitida ai vari size, gli LANCZOS resize sono
    # buoni ma ai size piccoli (16/24/32) il downscale diretto da 256
    # produce icone sfocate. La soluzione standard è generare ogni size
    # separatamente con resize LANCZOS dal sorgente, così Pillow li
    # impacchetta nel .ico mantenendo qualità ottimale per ciascuno.
    sizes_in_image = []
    images = []
    for sz in ICO_SIZES:
        if sz[0] > img.size[0]:
            print(f"[build_ico] skip {sz[0]}×{sz[1]} (sorgente troppo piccolo)")
            continue
        resized = img.resize(sz, Image.LANCZOS)
        images.append(resized)
        sizes_in_image.append(sz)

    if not images:
        print(f"ERRORE: nessuna size valida — sorgente troppo piccolo")
        sys.exit(1)

    # Pillow `save(format='ICO', sizes=...)` impacchetta tutte le
    # dimensioni in un unico file .ico. La prima `images[0]` è quella
    # principale, le altre sono passate via `append_images=`.
    dst_ico.parent.mkdir(parents=True, exist_ok=True)
    images[0].save(
        str(dst_ico),
        format="ICO",
        sizes=sizes_in_image,
        # IMPORTANTE: append_images serve solo per multi-frame come GIF,
        # NON per ICO. Per ICO basta `sizes=` con la lista — Pillow
        # genera il file con tutte le risoluzioni embedded.
    )

    actual_size = dst_ico.stat().st_size
    print(f"[build_ico] generato: {dst_ico} ({actual_size} byte)")
    print(f"[build_ico] dimensioni embedded: {sizes_in_image}")
    print(f"[build_ico] OK - ora rebuilda l'EXE con:")
    print(f"             pyinstaller tracklab.spec --clean")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", type=Path,
                        default=Path("icons/app/taskbar_active.png"),
                        help="PNG sorgente (default: icons/app/taskbar_active.png)")
    parser.add_argument("--dst", type=Path,
                        default=Path("icons/tracklab.ico"),
                        help="ICO di destinazione (default: icons/tracklab.ico)")
    args = parser.parse_args()
    generate_ico(args.src.resolve(), args.dst.resolve())


if __name__ == "__main__":
    main()
