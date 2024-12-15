import fitz  # PyMuPDF
import pathlib
import pymupdf4llm
import re

def extract_header_and_footer_from_pdf(pdf_path):
    """
    Extracts headers and footers from each page of a PDF and determines where processing should start.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        int: The page number to start further processing.
    """
    pdf_document = fitz.open(pdf_path)

    print("Extracting headers and footers from the PDF:\n")
    count_one_footer = 0

    for page_number in range(len(pdf_document)):
        page = pdf_document[page_number]
        text_blocks = page.get_text("blocks")

        page_height = page.rect.height
        header_threshold = page_height * 0.10  # Top 10% of the page
        footer_threshold = page_height * 0.90  # Bottom 10% of the page

        header_text = [
            block[4] for block in text_blocks if block[3] < header_threshold
        ]
        footer_text = [
            block[4] for block in text_blocks if block[1] > footer_threshold
        ]

        print(f"Page {page_number + 1}:")
        if header_text:
            print(f"  Header: {' | '.join(header_text).strip()}")
            print(header_text)
        else:
            print("  Header: None")

        if footer_text:
            print(f"  Footer: {' | '.join(footer_text).strip()}")
            # if int(footer_text[0]) == 1:
            #     count_one_footer += 1
            # if count_one_footer == 2:
            #     print(f"Returning page {page_number + 1} as starting point.")
            #     pdf_document.close()
            #     return page_number
        else:
            print("  Footer: None")

        print("-" * 50)

    # pdf_document.close()
    return len(pdf_document)

pdf_path = "student_guide.pdf"
output_md_path = "student_without_toc.md"


# Convert the PDF to Markdown starting from the determined page
extract_header_and_footer_from_pdf(pdf_path)