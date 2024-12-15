import fitz  # PyMuPDF
import pathlib
import pymupdf4llm

def pdf_to_markdown(pdf_path, output_md_path, start_page=0):
    pdf_document = fitz.open(pdf_path)
    pages_to_include = []
    # print(pdf_document)

    for page_number in range(start_page, len(pdf_document)):
        page = pdf_document[page_number]
        text = page.get_text()
        if text.strip():
            pages_to_include.append(page_number)
    pdf_document.close()
    md_text = pymupdf4llm.to_markdown(pdf_path, pages=pages_to_include)
    pathlib.Path(output_md_path).write_bytes(md_text.encode())
    print(f"Markdown file saved to {output_md_path}")
    
# def pdf_to_markdown_without_toc_and_headers(pdf_path, output_md_path, start_page=0):
#     """
#     Converts a PDF to Markdown, excluding TOC pages, headers, and footers while keeping structure (e.g., tables, headers).

#     Args:
#         pdf_path (str): Path to the PDF file.
#         output_md_path (str): Path to save the Markdown file.
#         start_page (int): The page number to start processing from.
#     """
#     # Open the PDF document
#     pdf_document = fitz.open(pdf_path)
#     pages_to_include = []
#     extracted_text = []

#     for page_number in range(start_page, len(pdf_document)):
#         page = pdf_document[page_number]
#         page_height = page.rect.height

#         # Extract text blocks with their bounding box coordinates
#         text_blocks = page.get_text("blocks")

#         # Define thresholds for headers and footers
#         header_threshold = page_height * 0.10  # Top 10% of the page
#         footer_threshold = page_height * 0.90  # Bottom 10% of the page

#         middle_content = []
#         for block in text_blocks:
#             block_text = block[4]
#             block_y0 = block[1]  # Top coordinate of the block
#             block_y1 = block[3]  # Bottom coordinate of the block

#             # Include only blocks within the "middle" region of the page
#             if block_y1 <= footer_threshold and block_y0 >= header_threshold:
#                 middle_content.append(block_text.strip())
#                 pages_to_include.append(page_number)

#         # Combine the middle content for the page
#         if middle_content:
#             extracted_text.append("\n".join(middle_content))
#             # pages_to_include.append(page_number)

#     pdf_document.close()

#     # Use pymupdf4llm to generate Markdown only for selected pages
#     md_text = pymupdf4llm.to_markdown(pdf_path, pages=pages_to_include)

#     # If necessary, modify the Markdown text here (e.g., remove additional unwanted content)
#     # For now, we will directly save it as is.

#     # Save the final Markdown to the output file
#     pathlib.Path(output_md_path).write_bytes(md_text.encode("utf-8"))
#     print(f"Markdown file saved to {output_md_path}")

# # Example usage
pdf_to_markdown("student_guide.pdf", "output.md", start_page=2)
