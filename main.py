from pypdf import PdfReader


def extract_text_from_pdf(pdf_path):
    # Open the PDF file
    with open(pdf_path, 'rb') as file:
        reader = PdfReader(file)

        # Iterate through each page
        for page in reader.pages:
            text = page.extract_text()

            # Split the text into lines
            lines = text.split('\n')

            # Iterate through each line
            for line in lines:
                # if line.__contains__("Madvare"):
                print(line)

# Example usage
pdf_path = "mvfodevarer.pdf"
extract_text_from_pdf(pdf_path)
