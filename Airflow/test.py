# import pymupdf4llm

# md_text = pymupdf4llm.to_markdown("course.pdf")

# # now work with the markdown text, e.g. store as a UTF8-encoded file
# import pathlib
# pathlib.Path("output.md").write_bytes(md_text.encode())

import fitz  # PyMuPDF
import pymupdf4llm
import pathlib

def extract_without_toc(pdf_path, toc_keywords=("สารบัญ", "Contents", "Table of Contents")):
    pdf_document = fitz.open(pdf_path)
    pages_to_exclude = []
    extracted_text = []

    for page_number in range(len(pdf_document)):
        page = pdf_document[page_number]
        text = page.get_text()
        
        # Check if the page is part of the TOC
        if any(keyword in text for keyword in toc_keywords):
            pages_to_exclude.append(page_number)
        else:
            extracted_text.append(text)
    
    pdf_document.close()
    return extracted_text, pages_to_exclude

def pdf_to_markdown_without_toc(pdf_path, output_md_path, toc_keywords=("สารบัญ", "Contents", "Table of Contents")):
    # Step 1: Filter out TOC pages
    pdf_document = fitz.open(pdf_path)
    pages_to_include = []
    for page_number in range(len(pdf_document)):
        page = pdf_document[page_number]
        text = page.get_text()
        
        # Check if this page is not part of the TOC
        if not any(keyword in text for keyword in toc_keywords):
            pages_to_include.append(page_number)
    pdf_document.close()

    # Step 2: Use PyMuPDF4LLM to convert non-TOC pages to Markdown
    md_text = pymupdf4llm.to_markdown(pdf_path, pages=pages_to_include)

    # Step 3: Save as a UTF-8 Markdown file
    pathlib.Path(output_md_path).write_bytes(md_text.encode())
    print(f"Markdown file saved to {output_md_path}")

# Usage
pdf_to_markdown_without_toc("course.pdf", "output.md")