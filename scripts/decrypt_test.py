"""Decrypt a single file from A.A.01.034 and print first 500 bytes."""

import zipfile
import zlib
from Crypto.Cipher import Blowfish

SERIES = "A.A.01.034"
ZIP_PATH = f"/Volumes/logbookdata/cnh/iso/repository/AGCE/data/series/{SERIES}/docs.zip"
KEY = b"\x00" * 24

def unpad_pkcs5(data: bytes) -> bytes:
    pad_len = data[-1]
    if pad_len < 1 or pad_len > 8:
        return data
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        return data
    return data[:-pad_len]

def decrypt(ciphertext: bytes) -> bytes:
    cipher = Blowfish.new(KEY, Blowfish.MODE_ECB)
    decrypted = cipher.decrypt(ciphertext)
    unpadded = unpad_pkcs5(decrypted)
    return zlib.decompress(unpadded)

with zipfile.ZipFile(ZIP_PATH) as z:
    xml_files = [n for n in z.namelist() if n.endswith(".xml")]
    print(f"Total XML files: {len(xml_files)}")

    # Try one from doc/ and one from iu/EN/ (per the Rust code)
    doc_files = [n for n in xml_files if n.startswith("doc/")]
    iu_files = [n for n in xml_files if n.startswith("iu/EN/")]
    print(f"  doc/ XMLs: {len(doc_files)}")
    print(f"  iu/EN/ XMLs: {len(iu_files)}")

    for label, target in [("DOC", doc_files[0]), ("IU", iu_files[0] if iu_files else None)]:
        if not target:
            print(f"\nNo {label} files found")
            continue
        print(f"\n=== {label}: {target} ===")
        raw = z.read(target)
        print(f"Encrypted: {len(raw)} bytes")
        xml = decrypt(raw)
        print(f"Decrypted+inflated: {len(xml)} bytes")
        text = xml.decode("utf-8", errors="replace")
        print(text[:500])
