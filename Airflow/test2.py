import fitz  # PyMuPDF
import pathlib
import pymupdf4llm
import re

def extract_footer_from_pdf(pdf_path):
    """
    Extracts only the footer text from each page of a PDF and determines where processing should start.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        int: The page number to start further processing.
    """
    pdf_document = fitz.open(pdf_path)

    print("Extracting footers from the PDF:\n")
    count_one = 0

    for page_number in range(len(pdf_document)):
        page = pdf_document[page_number]
        text_blocks = page.get_text("blocks")

        page_height = page.rect.height
        footer_threshold = page_height * 0.90

        footer_text = [
            block[4] for block in text_blocks if block[1] > footer_threshold
        ]

        print(f"Page {page_number + 1}:")
        if footer_text:
            print(f"  Footer: {' | '.join(footer_text).strip()}")
            if int(footer_text[0]) == 1:
                count_one += 1
            if count_one == 2:
                print(f"Returning page {page_number + 1} as starting point.")
                pdf_document.close()
                return page_number
        else:
            print("  Footer: None")
        print("-" * 50)

    len_pdf = len(pdf_document)
    pdf_document.close()
    return 0


def pdf_to_markdown_without_toc(pdf_path, output_md_path, start_page=0):
    """
    Converts a PDF to Markdown, excluding TOC pages and starting from a specific page.

    Args:
        pdf_path (str): Path to the PDF file.
        output_md_path (str): Path to save the Markdown file.
        start_page (int): The page number to start processing from.
    """
    pdf_document = fitz.open(pdf_path)
    pages_to_include = []

    for page_number in range(start_page, len(pdf_document)):
        page = pdf_document[page_number]
        text = page.get_text()
        # pages_to_include.append(page_number)
        if text.strip():  # Check if the page contains any text
            pages_to_include.append(page_number)
        else:
            print(f"Page {page_number + 1} is skipped as it does not contain readable text.")

        
    if not pages_to_include:
        print("No pages with readable text were found.")


    pdf_document.close()

    md_text = pymupdf4llm.to_markdown(pdf_path, pages=pages_to_include)

    test = pathlib.Path(output_md_path).write_bytes(md_text.encode())
    print(test)
    print(f"Markdown file saved to {output_md_path}")

# Example usage
pdf_path = "course.pdf"
output_md_path = "output.md"

# Determine the starting page
start_page = extract_footer_from_pdf(pdf_path)

# Convert the PDF to Markdown starting from the determined page
pdf_to_markdown_without_toc(pdf_path, output_md_path, start_page=start_page)
