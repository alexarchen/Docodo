using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
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

    public interface IIndexDocument : IEnumerable<IndexPage>, IEnumerator<IndexPage>
    {
        string Name { get; }

    }

    public interface IIndexDataSource : IEnumerable<IIndexDocument>, IEnumerator<IIndexDocument>, IDisposable
    {
        string Name { get;  }
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
    /* Simple text files data source */
    public class IndexTextFilesDataSource : IIndexDataSource
    {
        CancellationTokenSource cts;
        private Task navtask;
        private bool isNavigating = false;
        private AutoResetEvent _event = new AutoResetEvent(true);

        public int encodePage {get; set;}
        public IndexTextFilesDataSource(string path,string mod="*.txt",int EncodePage=1252)
        {
            cts = new CancellationTokenSource();
            Name = path;
            encodePage = EncodePage;
            isNavigating = true;
            filesToDo = new ConcurrentQueue<string>();
            navtask = Task.Factory.StartNew(()=> { Navigate(path, mod);
                isNavigating = false;
                _event.Set();
                Console.WriteLine("Navigation finished");
            },cts.Token);
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
        class IndexedFile : IIndexDocument
        {
            public IndexedFile(string fname, IndexTextFilesDataSource parent)
            {
                Name = fname;
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

            object IEnumerator.Current => Current;

            public bool MoveNext() { npage++; if (npage > 0) return (false);
                else
                {
                    
                    StreamReader sr = new StreamReader(Name, new Windows1251().GetEncoding(1251));
                    _current = new IndexPage(""+npage, sr.ReadToEnd());
                    sr.Close();

                }
                return (true); }
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

        IndexedFile _current;

        public IEnumerator<IIndexDocument> GetEnumerator()
        {
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
                    _current = new IndexedFile(str,this);
                    return (true);
                }
            }
            catch(Exception e)
            {

            }
            return (false);


        }

        public void Reset() {
            Console.WriteLine($"Warning! Calling of stub Reset() in {this.GetType()}!");
        }
        
    }
}