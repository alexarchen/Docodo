//DOCODO Search Engine
//Copyright(C) 2018  Alexey Zakharchenko
// https://github.com/alexarchen/Docodo

//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.

//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
//    GNU General Public License for more details.

//    You should have received a copy of the GNU General Public License
//    along with this program.If not, see<https://www.gnu.org/licenses/>.

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
        
        // for 0, header page returns map of fields
        public Dictionary<string,object> ConvertToObject(){
            Dictionary<string,object> ret = new Dictionary<string,object> ();
            StringReader sr = new StringReader(text);
            string line = "";
            while ((line = sr.ReadLine())!=null){
              string [] vals = line.Split('=');
              if (vals.Length==2){

                if (Regex.Match(vals[1],"[+-]?[0-9]+").Success)
                 ret.Add(vals[0],long.Parse(vals[1]));
                 else
                if (Regex.Match(vals[1],@"[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)").Success)
                 ret.Add(vals[0],decimal.Parse(vals[1]));
                 else
                 ret.Add(vals[0],vals[1]);
              }
            }

            return ret;
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
        string Name { get; }
        string Path { get; }
        void Reset(); // sets to initial counter, must be called before first call to Next()!!!
        float Estimate(); // estimate part done by enumerator
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

    /* Abstract Datasource which runs navigation in separate thread and 
     * manages documents via ConcurrentQueue */
    public abstract class QueuedDataSource<T> : IIndexDataSource
    {
        public QueuedDataSource(string name,string path){
            Name = name;
            Path = path;
         }
        protected long datasize=0;
        protected long datadone=0;
        // call when navigating to add to data size estemation
        protected virtual void AddDataSize(long sz)
        {
            Interlocked.Add(ref datasize, sz);
        }
        protected CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        protected ConcurrentQueue<T> filesToDo = new ConcurrentQueue<T>();

        // called when start navigation, must override
        protected abstract void Navigate(ConcurrentQueue<T> queue, CancellationToken token);
        public virtual float Estimate() { return datasize>0?datadone/datasize:0; }

        private AutoResetEvent _event = new AutoResetEvent(true);
        public string Name { get; set; } // unique name of datasource
        public string Path { get; protected set; } // path to navigate
        public bool IsNavigating { get; protected set; } = false;
        async Task NavigateAsync()
        {
            cancellationTokenSource = new CancellationTokenSource();
            IsNavigating = true;
            filesToDo = new ConcurrentQueue<T>();
            try
            {
                await Task.Run(() => { Navigate(filesToDo, cancellationTokenSource.Token); IsNavigating = false; }, cancellationTokenSource.Token);
            }
            catch (TaskCanceledException e)
            {

            }
            IsNavigating = false;
            _event.Set();
        }

        protected virtual void Enqueue(ConcurrentQueue<T> filesToDo,T item)
        {
            filesToDo.Enqueue(item);
        }

        virtual public IIndexDocument Next(bool wait)
        {

            T obj;
            if (!filesToDo.TryDequeue(out obj))
            {
                // Console.WriteLine($"TryDequee returns false {Navigating}, {urlsToDo.Count}");
                if (wait && IsNavigating)
                {
                    while (IsNavigating)
                    {
                        Thread.Sleep(100);
                        if (filesToDo.TryDequeue(out obj)) break;
                    }
                }

            }

            if (obj == null)
                return null;

            return DocumentFromItem(obj);

        }

        protected abstract IIndexDocument DocumentFromItem(T item);

        virtual public void Dispose()
        {
            cancellationTokenSource.Cancel();
            filesToDo = new ConcurrentQueue<T>();
        }

        public virtual void Reset()
        {
            if (IsNavigating) // remove previous task
            {
                cancellationTokenSource.Cancel();
                filesToDo = new ConcurrentQueue<T>();
            }
            datasize = 0;
            datadone = 0;
            NavigateAsync();

        }
    }




    /* Simple paginated text files data source */
    /* Single thread, single enumerator !!!! */
    public class IndexTextFilesDataSource : QueuedDataSource<IndexTextFilesDataSource.IndexedTextFile>, IIndexDirectDataSource
    {
        const int MAX_ITEMS = 1000000000;
        public int MaxItems = MAX_ITEMS; // set MaxItems before Reset()
        public string mod;
        public int encodePage {get; set;}
        
        /* Name - unique name, path - folder with txt files, mod - modificator, EncodePage - code page number */
        public IndexTextFilesDataSource(string Name, string path,string mod="*.txt",int EncodePage=1252): base(Name, path)
        {
            this.mod = mod;
            encodePage = EncodePage;
        }

          // not thread safe yet
        public IIndexDirectDocument this[string filename]
        {
            get
            {
                // filename is relative
                return new IndexedTextFile(Path.TrimEnd('\\')+"\\"+filename, this); 
            }
        }
        private int Count = 0;

        protected override void Navigate(ConcurrentQueue<IndexedTextFile> queue, CancellationToken token)
        {
            Navigate(queue, Path, mod);
        }

        protected override IIndexDocument DocumentFromItem(IndexedTextFile item)
        {
            return item;
        }

        void Navigate(ConcurrentQueue<IndexedTextFile> queue,string folder, string mod)
        {

            Console.WriteLine($"Nav {folder} start...");
            try
            {
                List<string> filelist = new List<string>();
                foreach (string modic in mod.Split(';'))
                    filelist.AddRange(Directory.GetFiles(folder, modic));
                foreach (String file in filelist)
                {
                    Console.WriteLine($"QUEUE {file}");
                    if (Count < MaxItems)
                    {
                        Enqueue(queue,new IndexedTextFile(file, this));
                        Count++;
                    }

                }
                

                string[] folders = Directory.GetDirectories(folder);

                foreach (string _folder in folders)
                {
                    Navigate(queue,_folder, mod);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
        }

        public class IndexedTextFile : IIndexDirectDocument, IEnumerator<IndexPage>
        {
            StreamReader sr = null;
            const int PAGE_SIZE = 3000;
            public string fname { get; protected set; }
            public IndexedTextFile(string fname, IIndexDataSource parent)
            {
                this.fname = fname;
                Name = fname.Substring(parent.Path.Length);// fname.IndexOfAny(new char[] { '\\', '/' }, parent.Path.Length) + 1);
                this.parent = parent;
            }
            public IIndexDataSource parent { get; private set; }
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
                            if (line.TrimStart(' ').StartsWith(";")) continue;
                            dict.Add(line.Split('=')[0], line.Split('=')[1].TrimEnd(new char[] { '\r', '\n' }));
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
                            headers.Add(line.Split('=')[0], line.Split('=')[1]);
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


    }

    /* Intermediate DataSource to cache passed text into a file */
    /* Single thread, single enumerator !!!! */
    /* Don't pass it to the Index, index creates it itself */
    public class IndexTextCacheDataSource: IIndexDirectDataSource
    {
        public string Name { get => source.Name; }
        public string Path { get => source.Path; }
        public string filename;
        public IIndexDataSource source { get; protected set; }
        // IEnumerator<IIndexDocument> enumerator;
        public virtual float Estimate() => source.Estimate();
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
        // Disposes only current datasource, do not disposes source
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
            int np = 0; // numer of pages processed

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
                        np++;

                        if ((np % 100) == 0)
                            parent.stream.FlushAsync();

                    }
                    return (true);
                }
                else
                {
                    parent.stream.FlushAsync();

                    return false;
                }

            }

            public void Reset() { np = 0; }

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
                if ((archive != null) && (archive.Mode==ZipArchiveMode.Create))
                {
                    Dispose();
                }

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
            archive = new ZipArchive(stream, ZipArchiveMode.Create);
        }

    }


}