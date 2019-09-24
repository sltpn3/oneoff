import pdfkit

url = ['https://r.sicabe.com/news/item/2556349846147074', 'https://r.sicabe.com/news/item/2556349699346434']

pdfkit.from_url(url, 'pdf_test.pdf')