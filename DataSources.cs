using HtmlAgilityPack;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
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
    public struct IndexPage
    {
        public string id;   // unique inside a document id
        public string text; // pure text of the page
        public IndexPage(string _id,string _text)
        {
            id = _id;
            text = _text;
        }
    };

    public interface IIndexDocument : IEnumerable<IndexPage>, IDisposable
    {
        /* Name must be unique inside source*/
        string Name { get; }

    }

    
    public interface IIndexDataSource: IDisposable //: IEnumerable<IIndexDocument>, IEnumerator<IIndexDocument>
    {
        /* Name must be unique inside index and short*/
        string Name { get;  }
        void Reset(); // sets to initial counter
        IIndexDocument Next(bool wait=true); // iterate next element
        
    }

    /* implement these interface to direct access from indext 
     * to documents text when return search results */
    public interface IIndexDirectDataSource: IIndexDataSource
    {
        IIndexDirectDocument this[string filename] { get; }
    }

    public interface IIndexDirectDocument : IIndexDocument
    {
        IndexPage this[string id] { get; }

    }


    /* Implementation */
    /*
    public class Windows1251 : EncodingProvider
    {
        public static Encoding GetEncoding()
        {
            return new Wind1251Encoding();
        }
        public override Encoding GetEncoding(string name)
        {
            if (name.Equals("windows-1251")) return (GetEncoding(1251));
            return null;
        }
        public override Encoding GetEncoding(int id)
        {
            return new Wind1251Encoding();

        }

        public class Wind1251Encoding : Encoding
        {
            public override int GetByteCount(char[] chars, int index, int count)
            {
                return (count);
            }


            public override int GetBytes(char[] chars, int charIndex, int charCount, byte[] bytes, int byteIndex)
            {
                for (int q = 0; q < charCount; q++)
                {
                    char c = chars[q + charIndex];

                    if (c < 0x7F) bytes[q+ byteIndex] = (byte)c;
                    else
                    {
                        if ((c >= 'а') && (c <= 'я')) bytes[q + byteIndex] = (byte)(0xE0 + (c - 'а'));
                        else
                        if ((c >= 'А') && (c <= 'Я')) bytes[q + byteIndex] = (byte)(0xC0 + (byte)(c - 'А'));
                        else if (c == 'ё') bytes[q + byteIndex] = 0xB8;
                        else if (c == 'Ё') bytes[q + byteIndex] = 0xA8;
                        else
                            bytes[q + byteIndex] = (byte)(c & 0xFF);
                    }
                    
                }
                return (charCount);
            }

            public override int GetCharCount(byte[] bytes, int index, int count)
            {
                return (count);
            }

            public override int GetChars(byte[] bytes, int byteIndex, int byteCount, char[] chars, int charIndex)
            {
                for (int q = 0; q < byteCount; q++)
                {
                    byte b = bytes[byteIndex + q];
                    if (b >= 0xC0)
                        chars[charIndex + q] = (char)(0x410 + (b - 0xC0));
                    else
                    if (b == 0xA8) chars[charIndex + q] = 'Ё';
                    else
                    if (b == 0xB8) chars[charIndex + q] = 'ё';
                    else
                        chars[charIndex + q] = (char)b;

                }
                return (byteCount);
            }

            public override int GetMaxByteCount(int charCount)
            {
                return (charCount);
            }

            public override int GetMaxCharCount(int byteCount)
            {
                return (byteCount);
            }
        }

    }
    */

    /* Class to simply represent text document for DOCODO index */
    public class IndexOnePageTextFile: IIndexDocument
    {
        private List<IndexPage> pages = new List<IndexPage>();
        public string Name { get; }
        public IndexOnePageTextFile(string name,string text)
        {
            Name = name;
            pages.Add(new IndexPage("0", text));
        }
        public IEnumerator<IndexPage> GetEnumerator()
        {
            return (pages.GetEnumerator());
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (this.GetEnumerator());
        }
        public void Dispose() { }

    }

    /* Simple paginated text files data source */
    /* Single thread, single enumerator !!!! */
    public class IndexTextFilesDataSource : IIndexDirectDataSource
    {
        CancellationTokenSource cts;
        private Task navtask;
        private bool isNavigating = false;
        public string mod;
        public string path;
        private AutoResetEvent _event = new AutoResetEvent(true);

        public int encodePage {get; set;}
        /* Name - unique name, path - folder with txt files, mod - modificator, EncodePage - code page number */
        public IndexTextFilesDataSource(string Name, string path,string mod="*.txt",int EncodePage=1252)
        {
            
            cts = new CancellationTokenSource();
            this.path = path;
            this.Name = Name;
            this.mod = mod;
            encodePage = EncodePage;
            filesToDo = new ConcurrentQueue<IndexedTextFile>();
        }

        virtual public void Dispose()
        {
            cts.Cancel();
            filesToDo.Clear();
        }

        ~IndexTextFilesDataSource()
        {
            cts.Cancel();

        }

        ConcurrentQueue<IndexedTextFile> filesToDo;

        // not thread safe yet
        public IIndexDirectDocument this[string filename]
        {
            get
            {
                // filename is relative
                return new IndexedTextFile(path.TrimEnd('\\')+"\\"+filename, this); 
            }
        }
        
        private void Navigate(string folder, string mod)
        {

            Console.WriteLine($"Nav {folder} start...");
            try
            {
                List<string> filelist = new List<string>();
                foreach (string modic in mod.Split(";"))
                    filelist.AddRange(Directory.GetFiles(folder, modic));
                foreach (String file in filelist)
                {
                    Console.WriteLine($"QUEUE {file}");
                    filesToDo.Enqueue(new IndexedTextFile(file,this));
                    _event.Set();

                }

                string[] folders = Directory.GetDirectories(folder);

                foreach (string _folder in folders)
                {
                    Navigate(_folder, mod);
                }

            }
            catch (Exception e)
            {

            }
        }

        public string Name { get; protected set; }
        public class IndexedTextFile : IIndexDirectDocument, IEnumerator<IndexPage>
        {
            StreamReader sr = null;
            const int PAGE_SIZE = 3000; 
            public string fname { get; protected set; }
            public IndexedTextFile(string fname, IndexTextFilesDataSource parent)
            {
                this.fname = fname;
                Name = fname.Substring(fname.IndexOfAny(new char[] { '\\', '/' },parent.path.Length)+1);
                this.parent = parent;
            }
            public IndexTextFilesDataSource parent { get; private set; }
            public string Name { get; private set; }
            private int npage = -1;
            protected IndexPage _current;
            
            public IndexPage Current {
                get { return (_current); }
            }
            IndexPage IEnumerator<IndexPage>.Current
            {
                get { return Current; }
            }
            public IndexPage this[string id] 
            {
                get
                {
                    char[] buff = new char[PAGE_SIZE];
                    if (sr == null) {
                        sr = GetStreamReader(fname);
                    }



                    if (sr != null)
                    {
                        sr.BaseStream.Seek(Int32.Parse(id) * PAGE_SIZE, SeekOrigin.Begin);
                        sr.DiscardBufferedData();
                        int iread;
                        if ((iread = sr.Read(buff, 0, PAGE_SIZE)) > 0)
                        {
                            return new IndexPage(id, new string(buff, 0, iread));
                        }
                    }
                    return new IndexPage(id, "");
                }
            }
            object IEnumerator.Current => Current;

            StreamReader GetStreamReader(string fname)
            {
                BinaryReader reader = new BinaryReader(File.OpenRead(fname));
                byte[] bytes = reader.ReadBytes(5000);
                /* https://github.com/errepi/ude/tree/master/src/Library */
                Ude.CharsetDetector detector = new Ude.CharsetDetector();
                detector.Feed(bytes, 0, 5000);
                detector.DataEnd();
                reader.Close();
                if (detector.Charset != null)
                {
                    try
                    {
                        return (new StreamReader(fname, Portable.Text.Encoding.GetEncoding(detector.Charset)));
                    }
                    catch (Exception e)
                    {

                    }
                }
               return new StreamReader(fname, true);

            }

            virtual public bool MoveNext() { npage++; 
                char[] buff = new char[PAGE_SIZE];
                if (sr == null) sr = GetStreamReader(fname);
                int iread = sr.Read(buff, 0, PAGE_SIZE);
                if (iread > 0)
                {
                    _current = new IndexPage("" + npage, new string(buff,0,iread));
                }
                else { return false; }

                return (true);
            }
            virtual public void Reset() {
                npage = -1;
                if (sr != null)
                {
                    sr.BaseStream.Seek(0, SeekOrigin.Begin);
                    sr.DiscardBufferedData();
                }
            }

            public IEnumerator<IndexPage> GetEnumerator()
            {
                return (this);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return (this);
            }

            virtual public void Dispose()
            {
                if (sr != null)
                { sr.Close(); sr = null; }
            }
        }


        public virtual IIndexDocument Next(bool wait = true)
        {
            try
            {
                if ((filesToDo.Count == 0) && (isNavigating) && (wait))
                {
                    _event.Reset();
                    _event.WaitOne();
                    // waiting until nask completed or current
                }
                IndexedTextFile file;
                if (filesToDo.TryDequeue(out file))
                {
                    return (file);
                }
            }
            catch (Exception e)
            {

            }
            return (null);

        }

        virtual public void Reset() {
            if (!isNavigating)
            {
                filesToDo.Clear();
                // start navigating
                isNavigating = true;
                navtask = Task.Factory.StartNew(() =>
                {
                    Navigate(path, mod);
                    isNavigating = false;
                    _event.Set();
                    Console.WriteLine("Navigation finished");
                }, cts.Token);
            }
        }

    }
    /* Intermediate DataSource to cache passed text in a file */
    /* Single thread, single enumerator !!!! */
    public class IndexTextCacheDataSource: IIndexDirectDataSource
    {
        public string Name { get => source.Name; }
        public string filename;
        IIndexDataSource source;
       // IEnumerator<IIndexDocument> enumerator;

        ZipArchive archive;
        private Stream stream=null;
        /* filename - cache file name */
        public IndexTextCacheDataSource(IIndexDataSource source,string filename)
        {
            this.filename = filename;
            this.source = source;

            if (File.Exists(filename))
            {
                try
                {
                    stream = File.Open(filename, FileMode.Open);
                    archive = new ZipArchive(stream, ZipArchiveMode.Read);
                }
                catch (Exception e)
                {
                    archive = null;
                    if (stream != null) stream.Close();
                    stream = null;
                }
            }
        }

        public void Dispose()
        {
            lock (ziplock)
            {
                if (archive != null) archive.Dispose();
                archive = null;
            }
            if (stream != null) stream.Close();
            stream = null;

        }

        public class TextCacheFile : IIndexDirectDocument, IEnumerator<IndexPage>
        {
            public TextCacheFile(string fname, IndexTextCacheDataSource parent)
            {
                // create when using direct access by parent[]
                Name = fname;
                this.parent = parent;
            }
            public TextCacheFile(IIndexDocument doc, IndexTextCacheDataSource parent)
            {
                // create when enumerating
                Name = doc.Name;
                this.parent = parent;
                this.doc = doc.GetEnumerator();
            }
            private IEnumerator<IndexPage> doc;
            public IndexTextCacheDataSource parent { get; private set; }
            public string Name { get; private set; }
            
            IndexPage IEnumerator<IndexPage>.Current
            {
                get => doc.Current;
            }
            public IndexPage this[string id]
            {
                get
                {
                    lock (parent.ziplock)
                    {
                        ZipArchiveEntry entry = parent.archive.GetEntry(Name + "{" + id + "}");
                        if (entry != null)
                        {
                            StreamReader reader = new StreamReader(entry.Open());
                            return new IndexPage(id, reader.ReadToEnd());
                        }
                    }
                    return new IndexPage(id, "");
                }
            }
            object IEnumerator.Current => doc.Current;

            public bool MoveNext()
            {

                // next page
                if (doc.MoveNext())
                {
                    IndexPage _current = doc.Current;
                    lock (parent.ziplock)
                    {
                        ZipArchiveEntry entry = parent.archive.CreateEntry(Name + "{" + _current.id + "}");
                        using (StreamWriter wr = new StreamWriter(entry.Open()))
                        {
                            wr.Write(_current.text);
                            wr.Close();
                        }
                    }
                    return (true);
                }
                else return false;

            }

            public void Reset() { }

            public IEnumerator<IndexPage> GetEnumerator()
            {
                return (this);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return (this);
            }

            public void Dispose()
            {
             
            }
        }

        public IIndexDirectDocument this[string file]
        {
            get
            {
                if ((File.Exists(filename)) && (archive==null))
                {
                    try
                    {
                        stream = File.Open(filename, FileMode.Open);
                        archive = new ZipArchive(stream, ZipArchiveMode.Read);
                    }
                    catch (Exception e)
                    {
                        archive = null;
                        if (stream != null) stream.Close();
                        stream = null;
                    }
                }

                // filename is relative
                if (archive!=null)
                 return new TextCacheFile(file, this);
                return null;
            }
        }
        /*
        public IIndexDocument Current
        { get {
                return _current;
            } }

        public bool MoveNext()
        {
            try
            {
                if (enumerator.MoveNext())
                {
                    _current = new TextCacheFile(enumerator.Current, this);
                    return (true);
                }
            }
            catch (Exception e)
            {

            }
            return (false);


        }
        */
        object ziplock = new object();

        public IIndexDocument Next(bool wait = true)
        {
            IIndexDocument doc = source.Next(wait);
            if (doc != null)
            {
                lock (ziplock)
                {
                    return (new TextCacheFile(doc, this));
                }
                
            }

            return null;
        }

        public void Reset()
        {
            if (source!=null) source.Reset();
            //if (archive != null) archive.Dispose();
            if (stream != null) stream.Close();
            File.Delete(filename);
            stream = File.Open(filename, FileMode.OpenOrCreate);
            archive = new ZipArchive(stream, ZipArchiveMode.Update);
        }

    }

    public class DocumentsDataSource: IndexTextFilesDataSource
    {
        public DocumentsDataSource(string Name,string path): base(Name,path,"*.pdf;*.txt",1251)
        {
            
        }

        public class IndexPDFDocument : IndexedTextFile
        {
            private PdfReader pdfReader = null;
            int npage = -1;
            string text = "";
            public IndexPDFDocument(string fname, IndexTextFilesDataSource parent) : base(fname, parent)
            {
                pdfReader  = new PdfReader(fname);

            }
            public IndexPDFDocument(string fname, Stream data, IndexTextFilesDataSource parent) : base(fname, parent)
            {
                pdfReader = new PdfReader(data);

            }

            override public bool MoveNext() 
            {
                if (npage < pdfReader.NumberOfPages-1)
                {
                    var result = new StringBuilder();
                    npage++;

                    ITextExtractionStrategy strategy = new SimpleTextExtractionStrategy();
                    string currentText = PdfTextExtractor.GetTextFromPage(pdfReader, npage+1, strategy);
                    currentText = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(currentText)));
                    _current = new IndexPage(""+npage,currentText);
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
                pdfReader.Close();
                pdfReader.Dispose();
            }

        }



        override public IIndexDocument Next(bool wait) 
        {
            IndexedTextFile file = (IndexedTextFile)base.Next(wait);

            if (file != null)
            {
                if (file.Name.ToLower().EndsWith(".pdf"))
                {
                    // PDF
                    return new IndexPDFDocument(file.fname,this);

                }
            }

            return (file);

        }
    }

    public class WebDataSource : IndexTextFilesDataSource
    {
        //public string Name { get; }
        HashSet<string> urlsAdded = new HashSet<string>();
        ConcurrentQueue<string> urlsToDo = new ConcurrentQueue<string>();
        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        //string baseurl { get; }
        private bool Navigating;
        public WebDataSource(string name,string url):base(name,url)
        {
            Name = name;
            path = new UriBuilder(url.Substring(0,url.LastIndexOf("/"))).ToString();
        }


        public override void Reset()
        {
            cancellationTokenSource.Cancel();
            cancellationTokenSource = new CancellationTokenSource();
            urlsAdded.Clear();
            urlsToDo.Clear();
            if (!Navigating)
            {
                Task.Factory.StartNew(() => { Navigating = true; Console.WriteLine($"Start web crawler from {path}"); TryAddUrl("/"); Navigate(path + "/"); Navigating = false; }, cancellationTokenSource.Token);
            }
        }

        void Navigate(string url)
        {
            var web = new HtmlWeb();
            web.UserAgent = "DOCODO";
            web.UsingCache = false;
            web.PreRequest += new HtmlWeb.PreRequestHandler((req) => {
                req.Headers.Add("accept", "text/html, text/plain");
                
                return true; });
            //web.PostResponse += new HtmlWeb.PostResponseHandler((req, resp) => {
            //});
            HtmlDocument html = web.Load(url);
            var nodes = html.DocumentNode.SelectNodes("//meta");
            if (nodes!=null)
            foreach (var node in nodes)
            {
                try
                {
                    if (node.Attributes["http-equiv"].Value.ToLower().Equals("refresh"))
                    {
                        var matches = Regex.Match(node.Attributes["content"].Value, @"url=([\w\.\\_\+\?\&]+)"); 
                        //string[] arr = node.Attributes["content"].Value.Split(';');
                        string s =TryAddUrl(matches.Groups[1].Value);
                            if (s != null)
                                Navigate(s);
                        }
                }
                catch (Exception e) { }
            }
            
            nodes = html.DocumentNode.SelectNodes("//a");
            if (nodes!=null)
            foreach (var node in nodes)
            {
                if (node.Attributes.Contains("href"))
                {
                    string s = TryAddUrl(node.Attributes["href"].Value);
                    if (s!=null)
                        Navigate(s);
                }
            }


        }

        private string TryAddUrl(string url)
        {
            string s = url.ToLower();
            if (s.Length == 0) return (null);
            if (s[0] == '#') return (null);

            if (!s.ToLower().StartsWith("http:"))
                s = path + (s.StartsWith('/')?"":"/")+ s;

            s = new UriBuilder(s).ToString();
            if (s.Length >= 4)
            {
                string ext = s.ToLower().Substring(s.Length - 4);
                if ((ext.Equals(".png")) || (ext.Equals(".svg")) || (ext.Equals(".jpg")) || (ext.Equals(".bmp")) || (ext.Equals(".gif")))
                    s = "";
            }

            if ((s.Length > 0) && (s.ToLower().StartsWith(path.ToLower())))
            {
                if (s.Length > 1024)
                    return null;

                if (!urlsAdded.Contains(s))
                {
                    urlsToDo.Enqueue(s);
                    urlsAdded.Add(s);
                    Console.WriteLine($"Parse url: {s}");
                    return (s);
                }

            }
            return (null);
        }
        override public IIndexDocument Next(bool bwait)
        {
            string str="";
            IIndexDocument ret = null;
            do
            {
                if (!urlsToDo.TryDequeue(out str))
                {
                    // Console.WriteLine($"TryDequee returns false {Navigating}, {urlsToDo.Count}");
                    if (bwait && Navigating)
                    {
                        while (Navigating)
                        {
                            if (urlsToDo.TryDequeue(out str)) break;
                            Thread.Sleep(100);
                        }
                    }

                }

                //Console.WriteLine($"TryDequee returns true {Navigating}, {urlsToDo.Count}");

                if (str == null) break; // nothing more or don't wait

                if ((str != null) && (str.Length > path.Length))
                {
                     HttpWebRequest req = HttpWebRequest.CreateHttp(str);
                     req.UserAgent = "DOCODO";
                     req.Accept = "text/html, text/plain, application/pdf";
                     req.Method = "GET";
                     WebResponse res;
                     try
                     {
                       res = req.GetResponse();
                     }
                     catch (WebException e)
                     {
                        continue;
                     }

                    /*using () */
                    {
                        if (res.ContentType.ToLower().Equals("application/pdf"))
                        {
                            ret = new DocumentsDataSource.IndexPDFDocument(str, res.GetResponseStream(), this);
                        }
                        else
                        {
                            HtmlDocument html = new HtmlDocument();
                            html.Load(res.GetResponseStream());
                            StringBuilder builder = new StringBuilder();
                            foreach (var node in html.DocumentNode.DescendantsAndSelf())
                            {
                                try
                                {
                                    if ((node.NodeType == HtmlNodeType.Text) && (!node.ParentNode.Name.Equals("script")) && (!node.ParentNode.Name.Equals("style")))
                                        builder.Append(node.InnerText + " ");
                                    else
                                      if (node.Name.Equals("img"))
                                    {
                                        builder.Append(node.Attributes["alt"].Value + " ");
                                    }
                                }
                                catch (Exception e) { }

                            }
                            // rectify text

                            string rstr = builder.ToString().Trim(new char[] { '\r', '\n', ' ' });
                            rstr = Regex.Replace(rstr, @"([ ]*[\n\r]+[ ]*)+", "\r\n");

                            if (rstr.Length > 0)
                                ret = new IndexOnePageTextFile(str.Substring(path.Length), rstr);
                        }
                    }
                }

            }
            while (ret == null);

           return ret;
        }

        override public void Dispose()
        {
            cancellationTokenSource.Cancel();
            urlsAdded.Clear();
            urlsToDo.Clear();
            Navigating = false;

        }
    }


}