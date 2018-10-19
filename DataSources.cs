using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
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

    public interface IIndexDocument : IEnumerable<IndexPage>, IEnumerator<IndexPage>, IDisposable
    {
        /* Name must be unique inside source*/
        string Name { get; }

    }

    
    public interface IIndexDataSource : IEnumerable<IIndexDocument>, IEnumerator<IIndexDocument>, IDisposable
    {
        /* Name must be unique inside index and short*/
        string Name { get;  }
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
            filesToDo = new ConcurrentQueue<string>();
        }

        public void Dispose()
        {
            cts.Cancel();
            filesToDo.Clear();
        }
        ~IndexTextFilesDataSource()
        {
            cts.Cancel();

        }

        ConcurrentQueue<string> filesToDo;

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
                foreach (String file in Directory.GetFiles(folder, mod))
                {
                    Console.WriteLine($"QUEUE {file}");
                    filesToDo.Enqueue(file);
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

        public string Name { get; private set; }
        public class IndexedTextFile : IIndexDirectDocument
        {
            StreamReader sr = null;
            const int PAGE_SIZE = 3000; 

            public IndexedTextFile(string fname, IndexTextFilesDataSource parent)
            {
                sr = new StreamReader(fname, new Windows1251().GetEncoding(1251));
                Name = fname.Substring(fname.IndexOf('\\',parent.path.Length)+1);
                this.parent = parent;
            }
            public IndexTextFilesDataSource parent { get; private set; }
            public string Name { get; private set; }
            private int npage = -1;
            private IndexPage _current;
            
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

            public bool MoveNext() { npage++; 
                char[] buff = new char[PAGE_SIZE];
                int iread = sr.Read(buff, 0, PAGE_SIZE);
                if (iread > 0)
                {
                    _current = new IndexPage("" + npage, new string(buff,0,iread));
                }
                else { return false; }

                return (true);
            }
            public void Reset() {
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

            public void Dispose()
            {
                if (sr != null)
                { sr.Close(); sr = null; }
            }
        }

        IndexedTextFile _current;

        public IEnumerator<IIndexDocument> GetEnumerator()
        {
            Reset();

            return (this);
        }

        object IEnumerator.Current => Current;

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (this);
        }

        public IIndexDocument Current
        { get  {  return _current; }   }

         public bool MoveNext()
         {
            try
            {
                if ((filesToDo.Count == 0) && (isNavigating))
                {
                    _event.Reset();
                    _event.WaitOne();
                    // waiting until nask completed or current

                }
                string str;
                if (filesToDo.TryDequeue(out str))
                {
                    _current = new IndexedTextFile(str,this);
                    return (true);
                }
            }
            catch(Exception e)
            {

            }
            return (false);


        }

        public void Reset() {
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
        IEnumerator<IIndexDocument> enumerator;

        ZipArchive archive;
        /* filename - cache file name */
        public IndexTextCacheDataSource(IIndexDataSource source,string filename)
        {
            this.filename = filename;
            this.source = source;
            if (File.Exists(filename))
            {
                try
                {
                    archive = new ZipArchive(File.Open(filename, FileMode.Open), ZipArchiveMode.Read);
                }
                catch (Exception e)
                {
                    archive = null;
                }
            }
        }

        public void Dispose()
        {
            if (archive != null) archive.Dispose();
            try
            {
                archive = new ZipArchive(File.Open(filename, FileMode.Open), ZipArchiveMode.Read);
            }
            catch (Exception e)
            {
                archive = null;
            }

        }

        public class TextCacheFile : IIndexDirectDocument
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
                this.doc = doc;
            }
            private IIndexDocument doc;
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
                    ZipArchiveEntry entry = parent.archive.GetEntry(Name + "{" + id + "}");
                    if (entry != null)
                    {
                        StreamReader reader = new StreamReader(entry.Open());
                        return new IndexPage(id,reader.ReadToEnd());
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
                    ZipArchiveEntry entry = parent.archive.CreateEntry(Name + "{" + _current.id + "}");
                    using (StreamWriter wr = new StreamWriter(entry.Open()))
                    {
                        wr.Write(_current.text);
                        wr.Close();
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

        TextCacheFile _current;

        public IEnumerator<IIndexDocument> GetEnumerator()
        {
            Reset();

            enumerator = source.GetEnumerator();
            return (this);
        }

        object IEnumerator.Current => Current;

        IEnumerator IEnumerable.GetEnumerator()
        {
           (source as IEnumerable).GetEnumerator();
            return (this);
        }

        public IIndexDirectDocument this[string filename]
        {
            get
            {
                // filename is relative
                return new TextCacheFile(filename, this);
            }
        }

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

        public void Reset()
        {
            if (source!=null) source.Reset();
            if (archive != null) archive.Dispose();
            File.Delete(filename);
            archive = new ZipArchive(File.Open(filename, FileMode.OpenOrCreate), ZipArchiveMode.Update);

        }

    }
}