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
        string Name { get; set; }

    }

    
    public interface IIndexDataSource: IDisposable //: IEnumerable<IIndexDocument>, IEnumerator<IIndexDocument>
    {
        /* Name must be unique inside index and short*/
        string Name { get;  }
        void Reset(); // sets to initial counter, must be called before first call to Next()!!!
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

    /* Class to simply hold read paged text document for DOCODO index */
    public class IndexPagedTextFile: IIndexDocument
    {
        public List<IndexPage> pages = new List<IndexPage>();
        public string Name { get; set; }
        public IndexPagedTextFile(string name,string text,string headers)
        {
            Name = name;
            pages.Add(new IndexPage("0", headers));
            pages.Add(new IndexPage("1", text));
        }
        public IEnumerator<IndexPage> GetEnumerator()
        {
            return (pages.GetEnumerator());
        }
        public void SetHeaders(string headers)
        {
            IndexPage p = pages[0];
            p.text = headers;
            pages[0] = p;
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
                Name = fname.Substring(fname.IndexOfAny(new char[] { '\\', '/' }, parent.path.Length) + 1);
                this.parent = parent;
            }
            public IndexTextFilesDataSource parent { get; private set; }
            public string Name { get;  set; }
            private int npage = -1;
            protected IndexPage _current;

            public IndexPage Current {
                get { return (_current); }
            }
            IndexPage IEnumerator<IndexPage>.Current
            {
                get { return Current; }
            }
            // page id is 1-based number 
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
                        int npage = Int32.Parse(id) - 1;
                        if ((npage < 0) || (npage * PAGE_SIZE > sr.BaseStream.Length)) { throw new InvalidOperationException("Page number is out of range"); }

                        sr.BaseStream.Seek(npage * PAGE_SIZE, SeekOrigin.Begin);
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
            static private void AddHeadersFromDscrFile(string filename, ref Dictionary<string, string> dict)
            {
                if (File.Exists(filename))
                {
                    try
                    {
                        StreamReader streamReader = new StreamReader(File.OpenRead(filename));
                        while (true)
                        {
                            string line = streamReader.ReadLine();
                            if (line == null) break;
                            if (line.TrimStart(' ').StartsWith(';')) continue;
                            dict.TryAdd(line.Split('=')[0], line.Split('=')[1].TrimEnd(new char[] { '\r', '\n' }));
                        }
                    }
                    catch (Exception e)
                    {
                    }
                }
            }

            static public string GetHeadersFromDscrFile(string filename, string baseheaders)
            {
                Dictionary<string, string> headers = new Dictionary<string, string>();
                try
                {
                    using (StringReader stringReader = new StringReader(baseheaders))
                    {
                        while (true)
                        {
                            string line = stringReader.ReadLine();
                            if (line == null) break;
                            headers.TryAdd(line.Split('=')[0], line.Split('=')[1]);
                        }
                    }
                } catch (Exception e) { }

                AddHeadersFromDscrFile(filename + ".dscr", ref headers);
                DirectoryInfo dir = (new FileInfo(filename)).Directory;
                do
                {
                    AddHeadersFromDscrFile(dir.FullName + "\\.dscr", ref headers);
                    dir = dir.Parent;
                }
                while (dir != null);

                StringBuilder b = new StringBuilder();
                foreach (string key in headers.Keys) b.Append($"{key}={headers[key]}\n");
                return b.ToString();
            }

            public delegate string HeadersFunc();
            public HeadersFunc headers;

            /* Override this function to set additional headers to file */
            public virtual string GetHeaders()
            {
              if (headers != null) return headers();
              return GetHeadersFromDscrFile(fname, "Name=" + Name + "\nSource="+parent.Name+"\n");
            }
            virtual public bool MoveNext() { npage++;

                if (npage == 0)
                {
                    // header page
                    _current = new IndexPage("" + npage, GetHeaders());
                }
                else
                {
                    char[] buff = new char[PAGE_SIZE];
                    if (sr == null) sr = GetStreamReader(fname);
                    int iread = sr.Read(buff, 0, PAGE_SIZE);
                    if (iread > 0)
                    {
                        _current = new IndexPage("" + npage, new string(buff, 0, iread));
                    }
                    else { return false; }
                }
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
    /* Intermediate DataSource to cache passed text into a file */
    /* Single thread, single enumerator !!!! */
    /* Don't pass it to the Index */
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
            public string Name { get; set; }
            
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


}