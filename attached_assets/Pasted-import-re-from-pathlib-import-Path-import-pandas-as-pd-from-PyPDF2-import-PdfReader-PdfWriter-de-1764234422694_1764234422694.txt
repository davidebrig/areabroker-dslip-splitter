import re
from pathlib import Path

import pandas as pd
from PyPDF2 import PdfReader, PdfWriter


def load_producers(excel_path: Path) -> pd.DataFrame:
    """
    Legge l'Excel nel formato che stai usando:
    - riga 1 = titolo
    - riga 2 = intestazioni
    - da riga 3 in poi, dati
    """
    df_raw = pd.read_excel(excel_path)

    df = df_raw.iloc[1:].copy()
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)

    df = df[["PRODUTTORE", "NUMERO", "CLIENTE"]].copy()

    df["NUMERO"] = df["NUMERO"].astype(str).str.upper().str.strip()
    df["CLIENTE"] = df["CLIENTE"].astype(str).str.upper().str.strip()

    return df


def extract_pages_from_pdf(pdf_path: Path, excel_numbers: set) -> pd.DataFrame:
    """
    Estrae per ogni pagina:
    - NUMERO (polizza) trovando il primo token sulla riga COMPAGNIA
      che esiste nella colonna NUMERO dell'Excel
    - CLIENTE (solo per info/debug)
    """
    reader = PdfReader(str(pdf_path))
    records = []

    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        numero = None
        cliente = None

        # Trova riga "COMPAGNIA ..."
        for line in text.splitlines():
            if line.startswith("COMPAGNIA"):
                tokens = line.split()
                # Cerchiamo tra i token dopo "COMPAGNIA" il primo che esiste in NUMERO (Excel)
                for tok in tokens[1:]:
                    cand = tok.strip().upper()
                    if cand in excel_numbers:
                        numero = cand
                        break
                break

        # CLIENTE (info)
        m = re.search(r"CLIENTE\s+([A-Z0-9' .,&/-]+)", text)
        if m:
            cliente = m.group(1).strip().upper()

        records.append(
            {
                "page": i + 1,  # pagine 1-based
                "NUMERO": numero,
                "CLIENTE": cliente,
            }
        )

    return pd.DataFrame(records)


def split_pdf_by_producer(pdf_path: Path, excel_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"PDF:   {pdf_path.name}")
    print(f"Excel: {excel_path.name}\n")

    # 1) Carica produttori da Excel
    df_prod = load_producers(excel_path)
    excel_nums = set(df_prod["NUMERO"].unique())

    # 2) Estrai NUMERO/CLIENTE da ogni pagina del PDF
    df_pdf = extract_pages_from_pdf(pdf_path, excel_nums)

    # 3) Merge solo su NUMERO
    df_merge = df_pdf.merge(df_prod, on="NUMERO", how="left")

    # 4) Pagine con almeno un produttore associato
    df_pages_prod = (
        df_merge[~df_merge["PRODUTTORE"].isna()]
        .loc[:, ["page", "NUMERO", "CLIENTE_x", "PRODUTTORE"]]
        .rename(columns={"CLIENTE_x": "CLIENTE"})
        .drop_duplicates(subset=["page", "PRODUTTORE"])
        .sort_values(["PRODUTTORE", "page"])
    )

    reader = PdfReader(str(pdf_path))

    # 5) Crea un PDF per ogni PRODUTTORE
    producers = sorted(df_pages_prod["PRODUTTORE"].unique())
    print(f"Trovati {len(producers)} produttori con almeno un DSLIP.\n")

    for prod in producers:
        writer = PdfWriter()
        pages_for_prod = df_pages_prod[df_pages_prod["PRODUTTORE"] == prod]["page"].unique()

        for p in pages_for_prod:
            writer.add_page(reader.pages[int(p) - 1])

        safe_name = prod.replace(" ", "_").replace(".", "").replace("&", "E")
        out_pdf = out_dir / f"dslip_{safe_name}.pdf"

        with open(out_pdf, "wb") as f:
            writer.write(f)

        print(f"- {prod}: {len(pages_for_prod)} pagine → {out_pdf.name}")

    # 6) DSLIP senza produttore
    matched_pages = set(df_pages_prod["page"].unique())
    all_pages = set(df_pdf["page"].unique())
    unmatched_pages = sorted(all_pages - matched_pages)

    print(f"\nDSLIP senza produttore: {len(unmatched_pages)} pagine.")

    if unmatched_pages:
        writer = PdfWriter()
        for p in unmatched_pages:
            writer.add_page(reader.pages[int(p) - 1])

        out_unmatched_pdf = out_dir / "dslip_SENZA_PRODUTTORE.pdf"
        with open(out_unmatched_pdf, "wb") as f:
            writer.write(f)

        df_unmatched = df_pdf[df_pdf["page"].isin(unmatched_pages)].copy()
        out_unmatched_xlsx = out_dir / "dslip_SENZA_PRODUTTORE_elenco.xlsx"
        df_unmatched.to_excel(out_unmatched_xlsx, index=False)

        print(f"- PDF senza produttore: {out_unmatched_pdf.name}")
        print(f"- Elenco Excel senza produttore: {out_unmatched_xlsx.name}")


def auto_find_files() -> tuple[Path, Path]:
    """
    In Replit: prende il primo PDF e il primo XLSX nella root.
    Se trova esattamente i nomi "elenco dslip completo" / "elenco produttori corretto"
    li preferisce.
    """
    root = Path(".")

    pdf_candidates = list(root.glob("*.pdf"))
    xlsx_candidates = list(root.glob("*.xlsx"))

    if not pdf_candidates:
        raise FileNotFoundError("Nessun PDF trovato nella root del progetto.")
    if not xlsx_candidates:
        raise FileNotFoundError("Nessun XLSX trovato nella root del progetto.")

    # Preferenze per nome
    pdf = None
    xlsx = None

    for p in pdf_candidates:
        name_low = p.name.lower()
        if "dslip" in name_low and "produttori" not in name_low:
            pdf = p
            break
    if pdf is None:
        pdf = pdf_candidates[0]

    for x in xlsx_candidates:
        name_low = x.name.lower()
        if "produttori" in name_low:
            xlsx = x
            break
    if xlsx is None:
        xlsx = xlsx_candidates[0]

    return pdf, xlsx


if __name__ == "__main__":
    try:
        pdf_path, excel_path = auto_find_files()
        out_dir = Path("output_dslip")
        split_pdf_by_producer(pdf_path, excel_path, out_dir)
        print("\nFatto. Controlla la cartella 'output_dslip' a sinistra.")
    except Exception as e:
        print("\nERRORE:", e)
