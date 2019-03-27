using HtmlAgilityPack;
using iText;
using iText.Kernel.Pdf;
using iText.Kernel.Pdf.Canvas.Parser;
using iText.Kernel.Pdf.Canvas.Parser.Listener;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;


namespace Docodo
{
    public class DocumentsDataSource : IndexTextFilesDataSource
    {
        public DocumentsDataSource(string Name, string path) : base(Name, path, "*.pdf;*.txt", 1251)
        {

        }

        public class IndexPDFDocument : IndexedTextFile
        {
            private PdfReader pdfReader = null;
            private PdfDocument pdfDocument = null;

            int npage = -1;
            string text = "";
            public IndexPDFDocument(string fname, IIndexDataSource parent) : base(fname, parent)
            {
                pdfReader = new PdfReader(fname);
                pdfDocument = new PdfDocument(pdfReader);

            }
            public IndexPDFDocument(string fname, Stream data, IIndexDataSource parent) : base(fname, parent)
            {
                pdfReader = new PdfReader(data);
                pdfDocument = new PdfDocument(pdfReader);

            }

            public override string GetHeaders()
            {
                if (headers != null) return headers();

                var result = new StringBuilder();


                if (pdfDocument.GetDocumentInfo().GetTitle().Length>0)
                    result.Append("Title=" + pdfDocument.GetDocumentInfo().GetTitle() + "\n");
                result.Append("Name=" + Name + "\n");
                if (pdfDocument.GetDocumentInfo().GetAuthor().Length>0)
                    result.Append("Author=" + pdfDocument.GetDocumentInfo().GetAuthor() + "\n");
                result.Append("Source=" + parent.Name + "\n");
                return GetHeadersFromDscrFile(fname, result.ToString());//Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(result.ToString()))));
            }

            override public bool MoveNext()
            {
                if (npage < pdfDocument.GetNumberOfPages()- 1)
                {
                    npage++;
                    if (npage == 0)
                    {
                        // header page
                        _current = new IndexPage("" + npage, GetHeaders());
                    }
                    else
                    {
                        ITextExtractionStrategy strategy = new SimpleTextExtractionStrategy();
                        PdfPage page = pdfDocument.GetPage(npage);
                        string currentText = PdfTextExtractor.GetTextFromPage(page, strategy);
                        currentText = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(currentText)));
                        _current = new IndexPage("" + npage, currentText);
                    }
                    return true;
                }
                else return (false);

            }

            public override void Reset()
            {
                base.Reset();
                npage = -1;
            }

            public override void Dispose()
            {
                pdfDocument.Close();
                pdfReader.Close();
               
            }

        }

        public static IIndexDocument FromFile(string file, IIndexDataSource parent)
        {
            string s = file.ToLower();
            if (s.EndsWith(".pdf"))
            {
                // PDF
                return new IndexPDFDocument(file, parent);

            }
            else
            if (s.EndsWith(".txt"))
            {
                return new IndexedTextFile(file, parent);
            }
            else
                if ((s.EndsWith(".html")) || (s.EndsWith(".html")))
            {

                using (FileStream fs = File.OpenRead(file))
                {
                    return WebDataSource.FromHtml(fs, file, parent.Name);
                }

            }

            return null;
        }

        protected override IIndexDocument DocumentFromItem(IndexedTextFile item)
        {
            IndexedTextFile file = (IndexedTextFile)base.DocumentFromItem(item);

            if (file != null)
            {
                return FromFile(file.fname, this);
/*
                if (file.Name.ToLower().EndsWith(".pdf"))
                {
                    // PDF
                    return new IndexPDFDocument(file.fname, this);

                }*/
            }

            return (file);
        }

    }

}