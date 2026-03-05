"""Shared decryption helpers for CNH Arbortext XML files."""

import zipfile
import zlib
from pathlib import Path

from Crypto.Cipher import Blowfish

SERIES_ROOT = Path("/Volumes/logbookdata/cnh/iso/repository/AGCE/data/series")
KEY = b"\x00" * 24


def unpad_pkcs5(data: bytes) -> bytes:
    """Remove PKCS5 padding from decrypted data."""
    pad_len = data[-1]
    if pad_len < 1 or pad_len > 8:
        return data
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        return data
    return data[:-pad_len]


def decrypt(ciphertext: bytes) -> bytes:
    """Blowfish ECB decrypt → PKCS5 unpad → zlib inflate."""
    cipher = Blowfish.new(KEY, Blowfish.MODE_ECB)
    decrypted = cipher.decrypt(ciphertext)
    unpadded = unpad_pkcs5(decrypted)
    return zlib.decompress(unpadded)


def decrypt_to_str(ciphertext: bytes) -> str:
    """Decrypt and decode to UTF-8 string."""
    return decrypt(ciphertext).decode("utf-8", errors="replace")


def zip_path(series: str) -> Path:
    """Return path to docs.zip for a series."""
    return SERIES_ROOT / series / "docs.zip"


def iter_iu_xmls(series: str, lang: str = "EN"):
    """Yield (filename, xml_string) for all IU files in a series zip.

    Filters to iu/{lang}/ files only.
    """
    prefix = f"iu/{lang}/"
    with zipfile.ZipFile(zip_path(series)) as z:
        for name in z.namelist():
            if name.startswith(prefix) and name.endswith(".xml"):
                try:
                    raw = z.read(name)
                    xml_str = decrypt_to_str(raw)
                    yield name, xml_str
                except Exception as e:
                    print(f"  FAIL {name}: {e}")
