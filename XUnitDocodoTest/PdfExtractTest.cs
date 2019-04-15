using PdfSharp.Pdf;
using PdfSharp.Pdf.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace XUnitDocodoTest
{
    public class PdfExtractTest
    {
        [Fact]
        public void SimpleTest()
        {
            foreach (string file in Directory.EnumerateFiles("../../../pdfs/", "*.pdf"))
            {
                PdfDocument doc = null;
                try
                {
                    doc  = PdfReader.Open(file, PdfDocumentOpenMode.ReadOnly);
                    {
                        StringBuilder ta = new StringBuilder();

                        using (PdfSharpTextExtractor.Extractor extractor = new PdfSharpTextExtractor.Extractor(doc))
                        {

                            int n = 0;
                            foreach (PdfPage page in doc.Pages)
                            {
                                extractor.ExtractText(page, ta);
                                ta.Append($"\n --- page {++n} ---- \n");
                            }
                        }
                        string text = ta.ToString();
                        Assert.NotEmpty(text);
                        File.WriteAllText(file + ".txt", text);

                    }
                    
                }
                catch (PdfSharp.Pdf.IO.PdfReaderException e)
                {
                    Console.WriteLine("PDFSharp exception: " + e.Message);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.Message);
                }
                finally
                {
                    if (doc != null) doc.Dispose();
                }

            }


        }

    }
}
