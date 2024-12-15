import re

def clean_markdown_file(input_md_path, output_md_path):
    """
    Cleans up a Markdown file by removing unwanted patterns such as individual numbers, symbols, and unwanted headers.
    
    Args:
        input_md_path (str): Path to the input Markdown file.
        output_md_path (str): Path to save the cleaned Markdown file.
    """
    # Read the input Markdown file
    with open(input_md_path, 'r', encoding='utf-8') as f:
        md_text = f.read()

    # Clean the Markdown content
    md_text = clean_markdown(md_text)

    # Write the cleaned content to the output file
    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(md_text)

    print(f"Cleaned Markdown file saved to {output_md_path}")

def clean_markdown(md_text):
    """
    Cleans up a Markdown text by removing individual numbers, symbols, and unwanted headers.
    
    Args:
        md_text (str): The Markdown content as a string.
    
    Returns:
        str: The cleaned Markdown text.
    """
    # Remove stray numbers like "###### 2 3 4 5 6..." and replace them with an empty string
    # md_text = re.sub(r'######\s*(\d[\s\d]*)', '', md_text)

    # Remove stray numbers at the start of lines (if they are not part of lists or headers)
    md_text = re.sub(r'\s*(\d+)\s*(?=\n)', '\n', md_text)
    md_text = re.sub(r'#### \s*(\d+)\s*(?=\n)', '\n', md_text)
    # md_text = re.sub(r'[\s]*', '\n', md_text)

    # Remove stray bullet points or symbols like "  " (if they are not part of lists)
    # md_text = re.sub(r'[\s]*', '', md_text)

    # Remove stray numbers that appear after headers (like "#### 71")
    # md_text = re.sub(r'####\s*(\d+)', '####', md_text)

    # Optionally, remove any stray headers with just numbers (like "#### 111")
    # md_text = re.sub(r'####\s*\d+', '####', md_text)

    return md_text

# Example usage
input_md_path = 'course_without_toc.md'  # Path to your input .md file
output_md_path = 'clean_course.md'  # Path to save the cleaned .md file

clean_markdown_file(input_md_path, output_md_path)
