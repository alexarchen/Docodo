using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace Docodo
{

    public class Document : Dictionary<string, string> { }

    public class XmlDataSource : QueuedDataSource<Document>
    {
        string xmlpath="";
        public XmlDataSource(string name,string path) : base(name,path)
        {
            xmlpath = path;
            int q = path.LastIndexOfAny(new char[]{'\\', '/'});
            if (q >= 0) Path = path.Substring(0, q+1);
        }
        void AddDocument (ConcurrentQueue<Document> queue,XmlReader reader)
        {
            Document doc = new Document();
            while (reader.Read() && reader.NodeType != XmlNodeType.EndElement)
            {
                AddField(reader, doc);
            }
            if (doc.ContainsKey("file"))
            {
                Console.WriteLine("Add file: " + doc["file"]);
              Enqueue(queue, doc);
            }
            else
                Console.WriteLine("Error xml: no file field in document");
        }

        void AddField (XmlReader reader, Document doc)
        {
            string name = reader.Name;
            reader.Read();
            if (reader.NodeType == XmlNodeType.Text)
            {
                doc.Add(name, reader.Value);
            }
            while ((reader.NodeType != XmlNodeType.EndElement) && (reader.Read()))
            { }
        }

        protected override void Navigate(ConcurrentQueue<Document> queue, CancellationToken token)
        {
            try
            {
                XmlTextReader reader = new XmlTextReader(xmlpath);
                reader.WhitespaceHandling = WhitespaceHandling.None;

                while (reader.Read())
                {
                    if (reader.NodeType == XmlNodeType.Element)
                    {
                        if (reader.Name.Equals("basepath"))
                        {
                            reader.Read();
                            if (reader.NodeType == XmlNodeType.Text)
                            {
                                if (reader.Value.Contains(":"))
                                    Path = reader.Value;
                                else
                                {
                                    int q = xmlpath.LastIndexOfAny(new char[] { '\\', '/' });
                                    if (q >= 0) Path = xmlpath.Substring(0, q + 1);
                                    else Path = "";
                                    Path+= reader.Value;

                                }
                            }
                            while ((reader.NodeType != XmlNodeType.EndElement) && (reader.Read()))
                            { }
                        }
                        else
                        if (reader.Name.Equals("document"))
                        {
                            AddDocument(queue, reader);
                        }
                    }
                    token.ThrowIfCancellationRequested();
                }
            }
            catch (Exception e)
            {
                if (e.GetType() != typeof(TaskCanceledException))
                    Console.WriteLine("Error in xml: " + e.Message);
                else throw e;
            }
            
         

        }

        protected override IIndexDocument DocumentFromItem(Document item)
        {
            // load document
            if (item.ContainsKey("file"))
            {
                string url = Path + item["file"];
                if (!url.Contains("://"))
                    return DocumentsDataSource.FromFile(url, this);
                else
                 return WebDataSource.FromUrl(url,this);
            }
            return null;

        }


    }
}
