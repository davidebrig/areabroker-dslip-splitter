import re
import io
import zipfile
from datetime import datetime

import pandas as pd
import streamlit as st
from PyPDF2 import PdfReader, PdfWriter


st.set_page_config(
    page_title="DSLIP Splitter",
    page_icon="📄",
    layout="wide"
)

if "processing_logs" not in st.session_state:
    st.session_state["processing_logs"] = []
if "manual_assignments" not in st.session_state:
    st.session_state["manual_assignments"] = {}
if "preview_data" not in st.session_state:
    st.session_state["preview_data"] = None
if "column_mapping" not in st.session_state:
    st.session_state["column_mapping"] = {
        "produttore": "PRODUTTORE",
        "numero": "NUMERO",
        "cliente": "CLIENTE"
    }
if "current_files_hash" not in st.session_state:
    st.session_state["current_files_hash"] = None


def get_files_hash(pdf_files, excel_file):
    if not pdf_files or not excel_file:
        return None
    pdf_names = sorted([pf.name for pf in pdf_files])
    return f"{excel_file.name}_{'-'.join(pdf_names)}"


def clear_session_for_new_files():
    st.session_state["manual_assignments"] = {}
    st.session_state["preview_data"] = None
    st.session_state["processed"] = False
    st.session_state["results"] = None
    st.session_state["show_preview"] = False


def add_log(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state["processing_logs"].append({
        "timestamp": timestamp,
        "level": level,
        "message": message
    })


def clear_logs():
    st.session_state["processing_logs"] = []


def load_producers_with_mapping(excel_file, col_mapping: dict) -> pd.DataFrame:
    df_raw = pd.read_excel(excel_file)
    
    df = df_raw.iloc[1:].copy()
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    
    prod_col = col_mapping.get("produttore", "PRODUTTORE")
    num_col = col_mapping.get("numero", "NUMERO")
    cli_col = col_mapping.get("cliente", "CLIENTE")
    
    df = df[[prod_col, num_col, cli_col]].copy()
    df.columns = ["PRODUTTORE", "NUMERO", "CLIENTE"]
    
    df["NUMERO"] = df["NUMERO"].astype(str).str.upper().str.strip()
    df["CLIENTE"] = df["CLIENTE"].astype(str).str.upper().str.strip()
    
    return df


def extract_pages_from_pdf(pdf_reader: PdfReader, excel_numbers: set, pdf_name: str = "") -> pd.DataFrame:
    records = []
    
    for i, page in enumerate(pdf_reader.pages):
        text = page.extract_text() or ""
        numero = None
        cliente = None
        raw_text_preview = text[:200].replace("\n", " ") if text else ""
        
        for line in text.splitlines():
            if line.startswith("COMPAGNIA"):
                tokens = line.split()
                for tok in tokens[1:]:
                    cand = tok.strip().upper()
                    if cand in excel_numbers:
                        numero = cand
                        add_log(f"PDF '{pdf_name}' Pag.{i+1}: Trovato NUMERO '{numero}'", "SUCCESS")
                        break
                break
        
        if numero is None:
            add_log(f"PDF '{pdf_name}' Pag.{i+1}: Nessun NUMERO trovato", "WARNING")
        
        m = re.search(r"CLIENTE\s+([A-Z0-9' .,&/-]+)", text)
        if m:
            cliente = m.group(1).strip().upper()
        
        records.append({
            "pdf_name": pdf_name,
            "page": i + 1,
            "NUMERO": numero,
            "CLIENTE": cliente,
            "text_preview": raw_text_preview
        })
    
    return pd.DataFrame(records)


def process_single_pdf(pdf_file, df_prod: pd.DataFrame, excel_nums: set):
    pdf_name = pdf_file.name
    add_log(f"Elaborazione PDF: {pdf_name}", "INFO")
    
    pdf_bytes = pdf_file.read()
    pdf_reader = PdfReader(io.BytesIO(pdf_bytes))
    total_pages = len(pdf_reader.pages)
    add_log(f"PDF '{pdf_name}': {total_pages} pagine trovate", "INFO")
    
    df_pdf = extract_pages_from_pdf(pdf_reader, excel_nums, pdf_name)
    
    return pdf_reader, pdf_bytes, df_pdf, total_pages


def process_files_batch(pdf_files, excel_file, progress_bar, status_text, col_mapping: dict, manual_assignments: dict):
    clear_logs()
    add_log("Inizio elaborazione batch", "INFO")
    
    status_text.text("Caricamento file Excel...")
    progress_bar.progress(5)
    
    df_prod = load_producers_with_mapping(excel_file, col_mapping)
    excel_nums = set(df_prod["NUMERO"].unique())
    add_log(f"Excel caricato: {len(df_prod)} righe, {len(excel_nums)} numeri unici", "INFO")
    
    all_pdf_data = []
    all_pdf_readers = {}
    total_pages_all = 0
    
    num_pdfs = len(pdf_files)
    for idx, pdf_file in enumerate(pdf_files):
        status_text.text(f"Lettura PDF {idx+1}/{num_pdfs}: {pdf_file.name}")
        progress_bar.progress(5 + int((idx + 1) / num_pdfs * 20))
        
        pdf_reader, pdf_bytes, df_pdf, total_pages = process_single_pdf(pdf_file, df_prod, excel_nums)
        all_pdf_readers[pdf_file.name] = PdfReader(io.BytesIO(pdf_bytes))
        all_pdf_data.append(df_pdf)
        total_pages_all += total_pages
    
    df_all_pdf = pd.concat(all_pdf_data, ignore_index=True) if all_pdf_data else pd.DataFrame()
    
    for (pdf_name, page), prod in manual_assignments.items():
        mask = (df_all_pdf["pdf_name"] == pdf_name) & (df_all_pdf["page"] == page)
        if mask.any():
            add_log(f"Assegnazione manuale: PDF '{pdf_name}' Pag.{page} -> {prod}", "INFO")
    
    status_text.text("Matching pagine con produttori...")
    progress_bar.progress(30)
    
    df_merge = df_all_pdf.merge(df_prod, on="NUMERO", how="left")
    
    for (pdf_name, page), prod in manual_assignments.items():
        mask = (df_merge["pdf_name"] == pdf_name) & (df_merge["page"] == page)
        df_merge.loc[mask, "PRODUTTORE"] = prod
    
    df_pages_prod = (
        df_merge[~df_merge["PRODUTTORE"].isna()]
        .loc[:, ["pdf_name", "page", "NUMERO", "CLIENTE_x", "PRODUTTORE"]]
        .rename(columns={"CLIENTE_x": "CLIENTE"})
        .drop_duplicates(subset=["pdf_name", "page", "PRODUTTORE"])
        .sort_values(["PRODUTTORE", "pdf_name", "page"])
    )
    
    status_text.text("Generazione PDF per produttore...")
    progress_bar.progress(40)
    
    producers = sorted(df_pages_prod["PRODUTTORE"].unique()) if not df_pages_prod.empty else []
    add_log(f"Trovati {len(producers)} produttori con pagine assegnate", "INFO")
    
    output_files = {}
    summary_data = []
    
    for idx, prod in enumerate(producers):
        writer = PdfWriter()
        pages_for_prod = df_pages_prod[df_pages_prod["PRODUTTORE"] == prod][["pdf_name", "page"]]
        
        page_count = 0
        for _, row in pages_for_prod.iterrows():
            pdf_name = row["pdf_name"]
            page_num = int(row["page"]) - 1
            if pdf_name in all_pdf_readers:
                writer.add_page(all_pdf_readers[pdf_name].pages[page_num])
                page_count += 1
        
        safe_name = prod.replace(" ", "_").replace(".", "").replace("&", "E")
        filename = f"dslip_{safe_name}.pdf"
        
        pdf_buffer = io.BytesIO()
        writer.write(pdf_buffer)
        pdf_buffer.seek(0)
        output_files[filename] = pdf_buffer.getvalue()
        
        summary_data.append({
            "Produttore": prod,
            "Pagine": page_count,
            "File": filename
        })
        
        add_log(f"Generato {filename}: {page_count} pagine", "SUCCESS")
        
        if producers:
            progress = 40 + int((idx + 1) / len(producers) * 40)
            progress_bar.progress(progress)
    
    status_text.text("Gestione pagine senza produttore...")
    progress_bar.progress(85)
    
    matched_keys = set(zip(df_pages_prod["pdf_name"], df_pages_prod["page"]))
    all_keys = set(zip(df_all_pdf["pdf_name"], df_all_pdf["page"]))
    unmatched_keys = all_keys - matched_keys
    
    unmatched_data = None
    unmatched_count = len(unmatched_keys)
    
    if unmatched_keys:
        writer = PdfWriter()
        for pdf_name, page in sorted(unmatched_keys):
            if pdf_name in all_pdf_readers:
                writer.add_page(all_pdf_readers[pdf_name].pages[int(page) - 1])
        
        pdf_buffer = io.BytesIO()
        writer.write(pdf_buffer)
        pdf_buffer.seek(0)
        output_files["dslip_SENZA_PRODUTTORE.pdf"] = pdf_buffer.getvalue()
        
        df_unmatched = df_all_pdf[
            df_all_pdf.apply(lambda r: (r["pdf_name"], r["page"]) in unmatched_keys, axis=1)
        ].copy()
        df_unmatched_export = df_unmatched[["pdf_name", "page", "NUMERO", "CLIENTE"]].copy()
        excel_buffer = io.BytesIO()
        df_unmatched_export.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        output_files["dslip_SENZA_PRODUTTORE_elenco.xlsx"] = excel_buffer.getvalue()
        
        unmatched_data = df_unmatched
        add_log(f"Pagine senza produttore: {unmatched_count}", "WARNING")
    
    progress_bar.progress(100)
    status_text.text("Elaborazione completata!")
    add_log("Elaborazione batch completata con successo", "SUCCESS")
    
    return {
        "output_files": output_files,
        "summary": pd.DataFrame(summary_data),
        "total_pages": total_pages_all,
        "matched_pages": len(matched_keys),
        "unmatched_pages": unmatched_count,
        "unmatched_data": unmatched_data,
        "producers_count": len(producers),
        "df_pdf": df_all_pdf,
        "df_prod": df_prod,
        "num_pdfs": num_pdfs
    }


def preview_extraction(pdf_files, excel_file, col_mapping: dict):
    clear_logs()
    add_log("Avvio anteprima estrazione", "INFO")
    
    df_prod = load_producers_with_mapping(excel_file, col_mapping)
    excel_nums = set(df_prod["NUMERO"].unique())
    
    all_preview_data = []
    
    for pdf_file in pdf_files:
        pdf_bytes = pdf_file.read()
        pdf_file.seek(0)
        pdf_reader = PdfReader(io.BytesIO(pdf_bytes))
        
        df_pdf = extract_pages_from_pdf(pdf_reader, excel_nums, pdf_file.name)
        
        df_merge = df_pdf.merge(df_prod[["NUMERO", "PRODUTTORE"]], on="NUMERO", how="left")
        all_preview_data.append(df_merge)
    
    if all_preview_data:
        return pd.concat(all_preview_data, ignore_index=True), df_prod
    return pd.DataFrame(), df_prod


def create_zip(output_files: dict) -> bytes:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename, content in output_files.items():
            zf.writestr(filename, content)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()


st.title("📄 DSLIP Splitter")
st.markdown("Dividi i tuoi PDF DSLIP per produttore in base al file Excel di mapping.")

with st.sidebar:
    st.header("⚙️ Configurazione")
    
    with st.expander("🔧 Mapping Colonne Excel", expanded=False):
        st.markdown("Personalizza i nomi delle colonne del tuo file Excel:")
        
        col_produttore = st.text_input(
            "Colonna Produttore",
            value=st.session_state["column_mapping"]["produttore"],
            key="col_prod_input"
        )
        col_numero = st.text_input(
            "Colonna Numero Polizza",
            value=st.session_state["column_mapping"]["numero"],
            key="col_num_input"
        )
        col_cliente = st.text_input(
            "Colonna Cliente",
            value=st.session_state["column_mapping"]["cliente"],
            key="col_cli_input"
        )
        
        if st.button("💾 Salva Mapping", use_container_width=True):
            st.session_state["column_mapping"] = {
                "produttore": col_produttore,
                "numero": col_numero,
                "cliente": col_cliente
            }
            st.success("Mapping salvato!")
    
    st.divider()
    
    if st.session_state["processing_logs"]:
        st.subheader("📋 Log Elaborazione")
        
        log_filter = st.selectbox(
            "Filtra per livello:",
            ["TUTTI", "INFO", "SUCCESS", "WARNING", "ERROR"],
            key="log_filter"
        )
        
        logs_to_show = st.session_state["processing_logs"]
        if log_filter != "TUTTI":
            logs_to_show = [l for l in logs_to_show if l["level"] == log_filter]
        
        log_container = st.container(height=300)
        with log_container:
            for log in reversed(logs_to_show[-50:]):
                level_icon = {
                    "INFO": "ℹ️",
                    "SUCCESS": "✅",
                    "WARNING": "⚠️",
                    "ERROR": "❌"
                }.get(log["level"], "📝")
                
                st.text(f"{log['timestamp']} {level_icon} {log['message']}")
        
        if st.button("🗑️ Cancella Log", use_container_width=True):
            clear_logs()
            st.rerun()

col1, col2 = st.columns(2)

with col1:
    st.subheader("📁 Carica file PDF DSLIP")
    pdf_files = st.file_uploader(
        "Trascina o seleziona uno o più file PDF con i DSLIP",
        type=["pdf"],
        key="pdf_upload",
        accept_multiple_files=True
    )
    if pdf_files:
        st.success(f"✓ {len(pdf_files)} file PDF caricati")
        for pf in pdf_files:
            st.caption(f"  • {pf.name}")

with col2:
    st.subheader("📊 Carica file Excel Produttori")
    excel_file = st.file_uploader(
        "Trascina o seleziona il file Excel con PRODUTTORE, NUMERO, CLIENTE",
        type=["xlsx", "xls"],
        key="excel_upload"
    )
    if excel_file:
        st.success(f"✓ {excel_file.name}")

if pdf_files and excel_file:
    new_hash = get_files_hash(pdf_files, excel_file)
    if st.session_state["current_files_hash"] != new_hash:
        clear_session_for_new_files()
        st.session_state["current_files_hash"] = new_hash

st.divider()

if pdf_files and excel_file:
    col_preview, col_process = st.columns(2)
    
    with col_preview:
        if st.button("🔍 Anteprima Estrazione", use_container_width=True):
            with st.spinner("Analisi in corso..."):
                try:
                    pdf_files_copy = []
                    for pf in pdf_files:
                        pf.seek(0)
                        pdf_files_copy.append(pf)
                    
                    preview_df, prod_df = preview_extraction(
                        pdf_files_copy, 
                        excel_file, 
                        st.session_state["column_mapping"]
                    )
                    st.session_state["preview_data"] = preview_df
                    st.session_state["preview_prod"] = prod_df
                    st.session_state["show_preview"] = True
                except Exception as e:
                    st.error(f"Errore durante l'anteprima: {str(e)}")
    
    with col_process:
        if st.button("🚀 Avvia Elaborazione", type="primary", use_container_width=True):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                pdf_files_copy = []
                for pf in pdf_files:
                    pf.seek(0)
                    pdf_files_copy.append(pf)
                excel_file.seek(0)
                
                results = process_files_batch(
                    pdf_files_copy, 
                    excel_file, 
                    progress_bar, 
                    status_text,
                    st.session_state["column_mapping"],
                    st.session_state["manual_assignments"]
                )
                
                st.session_state["results"] = results
                st.session_state["processed"] = True
                st.session_state["show_preview"] = False
                
            except Exception as e:
                st.error(f"Errore durante l'elaborazione: {str(e)}")
                st.exception(e)

if st.session_state.get("show_preview", False) and st.session_state.get("preview_data") is not None:
    st.divider()
    st.subheader("🔍 Anteprima Estrazione Numeri Polizza")
    
    preview_df = st.session_state["preview_data"]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        total_pages = len(preview_df)
        st.metric("📄 Pagine Totali", total_pages)
    with col2:
        matched = preview_df["NUMERO"].notna().sum()
        st.metric("✅ Con Numero", matched)
    with col3:
        unmatched = preview_df["NUMERO"].isna().sum()
        st.metric("❌ Senza Numero", unmatched)
    
    st.dataframe(
        preview_df[["pdf_name", "page", "NUMERO", "CLIENTE", "PRODUTTORE"]],
        use_container_width=True,
        hide_index=True
    )
    
    unmatched_preview = preview_df[preview_df["PRODUTTORE"].isna()]
    if not unmatched_preview.empty:
        with st.expander("⚙️ Assegnazione Manuale Pagine Senza Produttore", expanded=True):
            st.warning(f"Trovate {len(unmatched_preview)} pagine senza produttore. Puoi assegnarle manualmente:")
            
            prod_df = st.session_state.get("preview_prod", pd.DataFrame())
            producers_list = ["-- Non assegnato --"] + sorted(prod_df["PRODUTTORE"].unique().tolist()) if not prod_df.empty else ["-- Non assegnato --"]
            
            for idx, row in unmatched_preview.iterrows():
                key = (row["pdf_name"], row["page"])
                current_assignment = st.session_state["manual_assignments"].get(key, "-- Non assegnato --")
                
                col_info, col_select = st.columns([2, 1])
                with col_info:
                    st.text(f"PDF: {row['pdf_name']} | Pag. {row['page']} | Cliente: {row['CLIENTE'] or 'N/A'}")
                with col_select:
                    selected = st.selectbox(
                        "Produttore",
                        producers_list,
                        index=producers_list.index(current_assignment) if current_assignment in producers_list else 0,
                        key=f"assign_{row['pdf_name']}_{row['page']}",
                        label_visibility="collapsed"
                    )
                    if selected != "-- Non assegnato --":
                        st.session_state["manual_assignments"][key] = selected
                    elif key in st.session_state["manual_assignments"]:
                        del st.session_state["manual_assignments"][key]
            
            if st.session_state["manual_assignments"]:
                st.success(f"✓ {len(st.session_state['manual_assignments'])} assegnazioni manuali configurate")

if st.session_state.get("processed", False):
    results = st.session_state["results"]
    
    st.divider()
    st.subheader("📊 Riepilogo Elaborazione")
    
    cols = st.columns(5)
    
    with cols[0]:
        st.metric("📚 PDF Elaborati", results.get("num_pdfs", 1))
    
    with cols[1]:
        st.metric("📄 Pagine Totali", results["total_pages"])
    
    with cols[2]:
        st.metric("✅ Pagine Matched", results["matched_pages"])
    
    with cols[3]:
        st.metric("❌ Senza Produttore", results["unmatched_pages"])
    
    with cols[4]:
        st.metric("👥 Produttori", results["producers_count"])
    
    st.divider()
    
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("📋 Dettaglio per Produttore")
        if not results["summary"].empty:
            st.dataframe(
                results["summary"],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Nessun produttore trovato")
    
    with col_right:
        st.subheader("📥 Download")
        
        zip_data = create_zip(results["output_files"])
        
        st.download_button(
            label="⬇️ Scarica tutti i PDF (ZIP)",
            data=zip_data,
            file_name="dslip_output.zip",
            mime="application/zip",
            use_container_width=True,
            type="primary"
        )
        
        st.markdown("---")
        st.markdown("**Download singoli:**")
        
        for filename, content in results["output_files"].items():
            mime_type = "application/pdf" if filename.endswith(".pdf") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            st.download_button(
                label=f"📄 {filename}",
                data=content,
                file_name=filename,
                mime=mime_type,
                use_container_width=True
            )
    
    if results["unmatched_pages"] > 0:
        st.divider()
        with st.expander("⚠️ Pagine senza produttore - Dettaglio", expanded=False):
            st.warning(f"Trovate {results['unmatched_pages']} pagine senza produttore associato")
            if results["unmatched_data"] is not None:
                st.dataframe(
                    results["unmatched_data"][["pdf_name", "page", "NUMERO", "CLIENTE"]],
                    use_container_width=True,
                    hide_index=True
                )

    with st.expander("🔍 Dati estratti da tutti i PDF", expanded=False):
        st.dataframe(
            results["df_pdf"][["pdf_name", "page", "NUMERO", "CLIENTE"]],
            use_container_width=True,
            hide_index=True
        )
    
    with st.expander("📊 Mapping produttori (Excel)", expanded=False):
        st.dataframe(
            results["df_prod"],
            use_container_width=True,
            hide_index=True
        )

else:
    if not st.session_state.get("show_preview", False):
        st.info("👆 Carica i file PDF e Excel per iniziare l'elaborazione")

st.divider()
st.markdown("""
<div style="text-align: center; color: #666; font-size: 0.85em;">
    <strong>DSLIP Splitter</strong> - Dividi i tuoi PDF assicurativi per produttore
</div>
""", unsafe_allow_html=True)
