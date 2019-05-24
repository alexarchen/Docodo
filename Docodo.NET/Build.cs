using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using DynamicExpresso;
using System.Text;
using Iveonik.Stemmers;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Docodo
{
    public partial class Index
    {

        public int MaxTmpIndexItems { get; set; }

        public class Document
        {
            public string Name;
            public int nPages;
            public Document() { }
            public Document(string a, int n = 0) { Name = a; nPages = n; }
        }
        public class DocPage
        {
            public string id;
            public Document doc;
            public ulong coord; // base coordinate to calculate pos
            public DocPage() { }
            public DocPage(Document _doc, string _id) { doc = _doc; id = _id; }
        }

        // class IndexPageList used to define page by coordinate,
        // it remembers last GetPage's result and use it to speed up calculations
        class IndexPageList : Dictionary<ulong, DocPage>
        {
            List<Document> DocsList = new List<Document>();
            public void AddDocument(Document doc)
            {
                DocsList.Add(doc);
            }

            IEnumerator<KeyValuePair<ulong, DocPage>> _enum;

            ulong prevc = 0xFFFFFFFF;

            public void AddFromList(List<KeyValuePair<string, ulong>> list, ulong shift)
            {
                Document doc = new Document();

                foreach (KeyValuePair<string, ulong> d in list)
                {
                    if (d.Key[0] != DOC_SEP) // doc
                    {
                        doc = new Document(d.Key);
                        AddDocument(doc);
                    }
                    else
                    {
                        Add(d.Value + shift, new DocPage(doc, d.Key.Substring(1)));
                    }


                }

            }

            public DocPage GetPage(ulong coord)
            {


                if (prevc == 0xFFFFFFFF) { _enum = this.AsEnumerable().GetEnumerator(); _enum.MoveNext(); prevc = 0; }
                else
                {
                    //if (_enum.Current.Key == coord) return (_enum.Current.Value);
                    if ((_enum.Current.Key > coord) && (prevc <= coord)) return _enum.Current.Value;
                    if (_enum.Current.Key > coord) { _enum = this.AsEnumerable().GetEnumerator(); _enum.MoveNext(); prevc = 0; }
                }
                // do next iteration
                do
                {
                    if (_enum.Current.Key > coord)
                    {
                        _enum.Current.Value.coord = prevc;
                        return _enum.Current.Value;
                    }
                    prevc = _enum.Current.Key;
                } while (_enum.MoveNext());

                return (new DocPage());
            }

            public void Save(BinaryWriter bin)
            {
                if (this.Count() == 0) return;
                Document doc = new Document();
                foreach (KeyValuePair<ulong, DocPage> p in this)
                {
                    bin.Write(p.Key);
                    if (!doc.Equals(p.Value.doc))
                    {
                        doc = p.Value.doc;
                        bin.Write(doc.Name);
                        bin.Write(p.Key);
                        bin.Write(DOC_SEP + p.Value.id);
                    }
                    else
                    {
                        bin.Write(DOC_SEP + p.Value.id);
                    }
                }
            }
            public void Load(BinaryReader read)
            {
                DocsList = new List<Document>();

                try
                {
                    Document doc = new Document();
                    do
                    {
                        ulong n = read.ReadUInt64();
                        string s = read.ReadString();
                        if (s[0] != DOC_SEP)
                        {
                            doc = new Document(s);
                            DocsList.Add(doc);
                        }
                        else
                            Add(n, new DocPage(doc, s.Substring(1)));

                    }
                    while (true);
                }
                catch (Exception e)
                {
                    // probably endofstream exception
                }
                // prepare for search
                _enum = this.AsEnumerable().GetEnumerator();
                _enum.MoveNext();
            }
        }


        IndexPageList PagesList;

        protected virtual string GetSuffixCode(string word, string suf)
        {
            return null;// in new version
            string str = "";
            if (suf.Length <= 2)
                str = "" + WORD_SUFFIX_CHAR + word[0] + SUFFIX_DEVIDER_CHAR + suf;
            else
                str = "" + WORD_SUFFIX_CHAR + suf;
            return str;

        }

        // prepare words before adding to index
        // only low letters case
        virtual protected string PrepareWord(string word)
        {
            // TODO: replace multilanguage words, absurd words like bbbbbbb or brrrr
            return word;
        }
        // Get word codes based on vocs, stemmers
        // returns also suffixes
        virtual protected (string code, string suff)[] GetWordCodes(string word)
        {
            word = PrepareWord(word);
            if (word.Length == 0) return null;

            if ((word[0] <= '9') && (word[0] >= '0'))
                return new (string, string)[] { (word, null) };
            string stemmed = word;
            int nG = 0;
            int nVoc = 0;
            if (stopWords.Contains(word)) return new (string, string)[] { };
            string firststemmed = "";
            List<(string code, string suff)> l = new List<(string code, string suff)>();
            l.Add((word, null)); // add full form of word
            try
            {

                foreach (Vocab voc in vocs)
                {

                    if ((voc != null) && (word[0] >= voc.Range.begin) && (word[0] <= voc.Range.end) && ((stemmed = voc.Stem(word)) != null) && ((nG = voc.Search(stemmed)) != 0))
                    {
                        string str = FromInt((nVoc << 24) | (nG & Vocab.GROUP_NUMBER_MASK));
                        l.Add((str, null /* GetSuffixCode(word, word.Substring(stemmed.Length))*/));
                    }
                    if (firststemmed.Length == 0)
                        firststemmed = stemmed;
                    nVoc++;
                    //if (nG != 0) break;
                }

                if (nG == 0)
                {
                    // not know word

                    stemmed = firststemmed;
                    if (vocs.Count == 0)
                    {
                        // no vacabs loaded, use stemmers
                        foreach (var st in Stemmers)
                        {
                            if (!Regex.IsMatch(word, "[^" + st.range + "]"))
                            {
                                if (st.stemmer != null)
                                {
                                    try
                                    {
                                        stemmed = st.stemmer.Stem(word);
                                    }
                                    catch (Exception e)
                                    {
                                        throw;
                                    }
                                }
                                break;
                            }
                        }
                    }
                    //if (stemmed.Length == 0) //not found neither in vocs nor in stemmers
                    //    l.Add((word, null));
                    //else
                    if ((stemmed.Length > 0) && (!stemmed.Equals(word, StringComparison.Ordinal)))
                        l.Add((WORD_STEM_CHAR + stemmed, null /*GetSuffixCode(word, word.Substring(stemmed.Length))*/));
                }

            }
            catch (Exception e)
            {
                throw;
            }

            return l.ToArray();
        }

        /* Thread Safe index builder class */
        /* Used by Index.CreateAsync method
        /* Standalone use (for small amount of data, without datasources and texts): 
         * Index.Builder bldr = new Index.Builder(...).AddVoc().StopWords();
         * bldr.AddWord()
         * bldr.AddWord()
         * ....
         * Index index = bldr.Build();
         */
        public class Builder : SortedDictionary<string, IndexSequence.Builder>
        {
            public int nTmpIndex = 0;
            private static int nBuilder = 0; // holds unique builder id

            Index Parent;

            // create Builder using Index's settings from parent
            public Builder(Index parent) : base(new IndexComparer())
            {
                Parent = parent;
                MaxItems = parent.MaxTmpIndexItems;
                Path = System.IO.Path.Combine(parent.WorkPath, "" + (nBuilder++));
                Directory.CreateDirectory(Path);
            }
            // create Builder with new Index (path, InMem, vocs) and loading stop words from stopwordsfile
            public Builder(string path, bool InMem = false, Vocab[] vocs = null, string stopwordsfile = null) : base()
            {
                Parent = new Index(path, InMem, vocs);
                if (stopwordsfile != null)
                    Parent.LoadStopWords(stopwordsfile);
                MaxItems = Parent.MaxTmpIndexItems;
                Path = System.IO.Path.Combine(Parent.WorkPath, "" + (nBuilder++));
                Directory.CreateDirectory(Path);
            }


            public Builder StopWords(string file) { Parent.LoadStopWords(file); return this; }
            public Builder AddVoc(Vocab voc) { Parent.AddVoc(voc); return this; }



            public int MaxItems { get; }
            public string Path { get; }
            public int TotalCount { get; private set; } = 0;

            public ulong maxCoord { get; private set; } = 0; // maximum coordinate


            /// <summary>
            /// Write word into index builder 
            /// </summary>
            /// <param name="word">textual word</param>
            /// <param name="coord">coordinate of word in text space</param>
            public void AddWord(string word, ulong coord)
            {
                //uint d = 0;
                var codes = Parent.GetWordCodes(word);
                if (codes != null)
                    foreach (var code in codes)
                    {
                        Add(code.code, coord);
                        if ((Parent.bKeepForms) && (code.suff != null))
                        {
                            Add(code.suff, coord + 1);
                        }
                    }

            }

            /// <summary>
            /// Add word code into Index Builder. Coord must be greater with each call.
            /// Usually you should use AddWord to add word
            /// </summary>
            /// <param name="code">unique word code</param>
            /// <param name="coord">coordinate in text space</param>
            public void Add(string code, ulong coord)
            {
                maxCoord = coord; // coord increases with each call
                IndexSequence.Builder val;
                if (!base.TryGetValue(code, out val))
                {
                    val = new IndexSequence.Builder();
                    base.Add(code, val);
                }
                val.Add(coord);
                TotalCount++;
                if (TotalCount > MaxItems)
                {
                    Save(false);
                    Clear();
                    TotalCount = 0;
                }
            }

            List<KeyValuePair<string, ulong>> pages = new List<KeyValuePair<string, ulong>>();

            // Add document, must be called before AddWord
            // sourceid - unique source name, must not be empty
            // AddDoc() .... EndPage("1")... EndPage("X") AddDoc()...  EndPage("X") Save() or Build()
            public void AddDoc(string sourceid, string name)
            {
                if (sourceid.Length == 0) throw new Exception("sourceid must not be empty");
                AddDoc(sourceid, name, maxCoord);
            }
            public void AddDoc(string sourceid, string name, ulong maxcoord)
            {
                pages.Add(new KeyValuePair<string, ulong>(sourceid + DOC_SEP + name, maxcoord));
            }
            // Finish page and start next one
            // AddDoc() .... EndPage("1")... EndPage("X") AddDoc()... EndPage("X") Save() or Build()
            public void EndPage(string id)
            {
                EndPage(id, maxCoord);
            }

            public void EndPage(string id, ulong maxcoord)
            {
                pages.Add(new KeyValuePair<string, ulong>(DOC_SEP + id, maxcoord));
            }


            public void Save(bool bSavePages = true)
            {
                try
                {
                    Interlocked.Increment(ref nTmpIndex);
                    FileStream file = File.Create(System.IO.Path.Combine(Path, $"{nTmpIndex}.tmpind"));
                    BinaryWriter bin = new BinaryWriter(file);
                    bin.Write(maxCoord);

                    foreach (KeyValuePair<string, IndexSequence.Builder> item in this)
                    {
                        bin.Write(item.Key);
                        IndexSequence seq = item.Value;
                        seq.Write(bin);
                    }

                    bin.Close();
                    //file.Close();


                    if (bSavePages)
                    {
                        FileStream fileStream = File.Create(System.IO.Path.Combine(Path, $"index.tmplist"));
                        BinaryFormatter binf = new BinaryFormatter();
                        binf.Serialize(fileStream, pages);
                        fileStream.Close();

                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error saving TempIndex: " + e.Message);
                }

            }

            /* Builds parent and returns it */
            public Index Build()
            {
                if (nTmpIndex != 0) throw new Exception("Can't build, index is too large");

                if (pages.Count == 0) { AddDoc("", "", 0); EndPage("1"); }

                lock (Parent.DoSearchLock)
                {
                    Save();

                    Parent.Dispose();

                    // Copy files 
                    if (File.Exists(System.IO.Path.Combine(Parent.WorkPath, ".index"))) File.Delete(System.IO.Path.Combine(Parent.WorkPath, ".index"));
                    if (File.Exists(System.IO.Path.Combine(Parent.WorkPath, ".index.list"))) File.Delete(System.IO.Path.Combine(Parent.WorkPath, ".index.list"));
                    File.Move(System.IO.Path.Combine(Path, "1.tmpind"), System.IO.Path.Combine(Parent.WorkPath, ".index"));

                    Parent.PagesList = new IndexPageList();
                    Parent.PagesList.AddFromList(pages, 0);

                    BinaryWriter binOut = new BinaryWriter(File.Create(System.IO.Path.Combine(Parent.WorkPath, ".index.list")));
                    Parent.PagesList.Save(binOut);
                    binOut.Close();
                    Parent.Load();
                }

                return Parent;
            }


        };

        public string WorkPath;



        public Builder GetBuilder() { return new Builder(this); }

        private void IndexTask(IIndexDataSource source)
        {


            try
            {
                // main index task
                Builder index = GetBuilder();

                Console.WriteLine("Started IndexTask id{0} in {1}", Task.CurrentId, index.Path);

                // task to process files


                ulong coord = 0;
                do
                {

                    //   if (cancel.Token.IsCancellationRequested) break;

                    using (IIndexDocument doc = source.Next())
                    {

                        if (doc == null) break;

                        Console.WriteLine("ID:{0} <-{1}", Task.CurrentId, doc.Name);

                        try
                        {
                            // TODO: File must fit RAM
                            index.AddDoc(source.Name, doc.Name);

                            foreach (IndexPage page in doc)
                            {

                                try
                                {
                                    String c = page.text.ToLower();
                                    if (c.Length == 0) continue;

                                    if (page.id.Equals("0"))
                                    {
                                        string pagetext = page.text;
                                        // headers page
                                        using (StringReader sr = new StringReader(pagetext))
                                        {
                                            try
                                            {
                                                while (true)
                                                {
                                                    string line = sr.ReadLine();

                                                    if (line == null) break;
                                                    line = line.ToLower();
                                                    string[] fields = line.Trim('\n').Split('=');
                                                    if (fields[0].Length >= MIN_WORD_LENGTH)
                                                    {
                                                        var matches = Regex.Split(fields[1], "\\b");//Regex.Matches(fields[1], @"\b\w+\b");
                                                        int dc = fields[0].Length + 1;
                                                        foreach (var match in matches)
                                                        {
                                                            if ((match.Length >= 1) && (IsLetterOrDigit(match[0]))) //&& (!stopWords.Contains(match)))
                                                            {
                                                                //coord += (ulong)(fields[0].Length);
                                                                index.Add(FIELD_NAME_CHAR + fields[0], (uint)(coord + (ulong)(dc - 1)));
                                                                index.AddWord(match.ToLower(), coord + (ulong)dc);
                                                            }
                                                            dc += match.Length;
                                                        }
                                                    }
                                                    coord += (ulong)line.Length + 1; // +1 from \n
                                                }
                                            }
                                            catch (EndOfStreamException e)
                                            { }
                                        }
                                        index.EndPage(page.id, coord);

                                        continue;
                                    }

                                    foreach (Match m in Regex.Matches(c, @"\p{L}+|\p{N}+"))
                                    {
                                        if ((m.Value.Length >= MIN_WORD_LENGTH) && (m.Value.Length <= MAX_WORD_LENGTH))
                                            index.AddWord(m.Value, coord + (uint)m.Index);
                                    }
                                    coord += (uint)c.Length;
                                    index.EndPage(page.id, coord);
                                    //         nextCoord.Add(coord,cancel.Token);
                                    //       Console.WriteLine($"Task {Task.CurrentId} pushed next coord {coord}");

                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Error parsing file {0}: {1}", doc.Name, e.Message);
                                }

                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Error in doc " + doc.Name+": "+e.Message);
                        }

                    }
                }
                while (true);

                index.Save();


            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception in Task{Task.CurrentId}: " + e.Message);
            }


            Console.WriteLine($"Task{Task.CurrentId} exited");
        }
    }
}