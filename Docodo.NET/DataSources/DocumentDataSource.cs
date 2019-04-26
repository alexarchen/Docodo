using HtmlAgilityPack;
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
using PdfSharp;
using PdfSharpTextExtractor;
using PdfSharp.Pdf;
using PdfSharp.Pdf.IO;

namespace Docodo
{
    public class DocumentsDataSource : IndexTextFilesDataSource
    {
        public DocumentsDataSource(string Name, string path) : base(Name, path, "*.pdf;*.txt", 1251)
        {

        }

        public class IndexPDFDocument : IndexedTextFile
        {
            private PdfDocument pdfDocument = null;
            private Extractor pdfExtractor=null;

            int npage = -1;
            string text = "";
            public IndexPDFDocument(string fname, IIndexDataSource parent) : base(fname, parent)
            {
                try
                {
                    pdfDocument = PdfReader.Open(fname, PdfDocumentOpenMode.ReadOnly);
                    pdfExtractor = new Extractor(pdfDocument);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error open pdf: {fname}");
                }
            }
            public IndexPDFDocument(string fname, Stream data, IIndexDataSource parent) : base(fname, parent)
            {
                try
                {
                    pdfDocument = PdfReader.Open(data, PdfDocumentOpenMode.ReadOnly);
                pdfExtractor = new Extractor(pdfDocument);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error open pdf: {fname}");
                }
            }

            public override string GetHeaders()
            {
                if (headers != null) return headers();

                var result = new StringBuilder();

                
                if (pdfDocument.Info.Title.Length>0)
                    result.Append("Title=" + pdfDocument.Info.Title + "\n");
                result.Append("Name=" + Name + "\n");
                if (pdfDocument.Info.Author.Length>0)
                    result.Append("Author=" + pdfDocument.Info.Author + "\n");
                result.Append("Source=" + parent.Name + "\n");
                return GetHeadersFromDscrFile(fname, result.ToString());//Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(result.ToString()))));
            }

            override public bool MoveNext()
            {
                if (pdfDocument == null) return false;

                if (npage < pdfDocument.PageCount- 1)
                {
                    npage++;
                    if (npage == 0)
                    {
                        // header page
                        _current = new IndexPage("" + npage, GetHeaders());
                    }
                    else
                    {
                        PdfPage page = pdfDocument.Pages[npage];
                        StringBuilder text = new StringBuilder();
                        pdfExtractor.ExtractText(page, text);
                        //currentText = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(currentText)));
                        _current = new IndexPage("" + npage, text.ToString());
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
                if (pdfExtractor != null)
                    pdfExtractor.Dispose();

                if (pdfDocument!=null)
                pdfDocument.Dispose();
               
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