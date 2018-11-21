using Docodo;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace XUnitDocodoTest
{
    public class DataSourceTest 
    {

        [Fact]
        public void DocumentDataSourceTest()
        {
            /* Create folder Test in DOCODO root folder, place >=2 documents in it */
            string path = "..\\..\\..\\..\\Test\\";
            string [] files = Directory.GetFiles(path);
            DocumentsDataSource ds = new DocumentsDataSource("doc", path);
            ds.Reset();
            Task.Factory.StartNew<IIndexDocument>(() => { return ds.Next(true); }).ContinueWith((doc) =>
            {
                Assert.NotNull(doc.Result);
                Assert.IsAssignableFrom<IIndexDocument>(doc.Result);
                Assert.Contains(path+((IIndexDocument)doc.Result).Name, files);
                int c = 0;
                foreach (IndexPage p in ((IIndexDocument)doc.Result))
                {
                    c++;
                }
                Assert.True(c > 10);

            }).Wait();
            Task.Factory.StartNew<IIndexDocument>(() => ds.Next(true)).ContinueWith((doc) =>
            {
                Assert.NotNull(doc.Result);
                Assert.IsAssignableFrom<IIndexDocument>(doc.Result);
                Assert.Contains(path + ((IIndexDocument)doc.Result).Name, files);
                int c = 0;
                foreach (IndexPage p in ((IIndexDocument)doc.Result))
                {
                    c++;
                }
                Assert.True(c > 10);
            }).Wait();

        }

        [Fact]
        public void XmlDataSourceTest()
        {
            /* Create folder Test in DOCODO root folder, place >=2 documents in it,
             * create test.xml file in root folder with description of documents */

            XmlDataSource xml = new XmlDataSource("xml", "..\\..\\..\\..\\test.xml");
            xml.Reset();
            Task.Factory.StartNew<IIndexDocument>(() => { return xml.Next(true); }).ContinueWith((doc) =>
             {
              Assert.NotNull(doc.Result);
              Assert.IsAssignableFrom< IIndexDocument>(doc.Result);
              Assert.Equal("Dickens Charles. The Pickwick Papers - royallib.ru.txt", ((IIndexDocument)doc.Result).Name);
                 int c = 0;
                 foreach (IndexPage p in ((IIndexDocument)doc.Result))
                 {
                     c++;
                 }
                 Assert.True(c > 10);

             }).Wait();
            Task.Factory.StartNew<IIndexDocument>(() => xml.Next(true)).ContinueWith((doc) =>
            {
                Assert.NotNull(doc.Result);
                Assert.IsAssignableFrom<IIndexDocument>(doc.Result);
                Assert.Equal("PDFSPEC.PDF", ((IIndexDocument)doc.Result).Name);
                int c = 0;
                foreach (IndexPage p in ((IIndexDocument)doc.Result))
                {
                    c++;
                }
                Assert.True(c > 10);
            }).Wait();

            Assert.Null(xml.Next(true));
        }

    }
}
