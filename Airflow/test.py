# -------------------- not Include layout but fast --------------------
# import pdfplumber
# from docx import Document
# from docx.shared import Inches

# # Open a PDF file
# with pdfplumber.open("./include/test.pdf") as pdf:

#     # Extract text from the PDF
#     text = ""
#     for page in pdf.pages:
#         text += page.extract_text()
 
# # Create a new Word document
# document = Document()

# # Add a paragraph in Word to hold the text
# document.add_paragraph(text)

# # Save the Word document
# document.save("output.docx")


# --------------------- Include layout but slow ----------------------

# from pdf2docx import Converter

# def convert_pdf_to_docx(pdf_file, docx_file):

#     # Create a Converter object
#     cv = Converter(pdf_file)

#     # Convert specified PDF page to docx 
#     cv.convert(docx_file, start=0, end=None)
#     cv.close()

# # Convert a PDF to a Docx file
# convert_pdf_to_docx("C:\\Users\\Administrator\\Desktop\\Input.pdf", "Output.docx")