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


    public class Index : IDisposable
    {
        const int MAX_DEF_TMP_INDEXITEMS = 1000001; // Maximum items in tempindex
        const int MAX_WORD_LENGTH = 32;      // Maximum word length
        const long MAX_FILE_SIZE = 200000000; // Maximum indexable text file size
        const int COORD_DEVIDER = 1; // devider of coordinates in index
        const int SUBWORD_LENGTH = 4; // words in index are splittered by groups of this number of chars
        const int MAX_FOUND_PAGES = 30000; // maximum output found pages
        const int MAX_FOUND_DOCS = 500; // maximum output found docs
        const int MAX_FOUND_PAGES_IN_DOC = 1000; // maximum output found pages in one document
        const char WORD_SUFFIX_CHAR = '$'; // char prefix of word stemming remainder
        const char SUFFIX_DEVIDER_CHAR = ':';
        const char WORD_BEGIN_CHAR = '<';
        const char WORD_END_CHAR = '>';
        const char KNOWN_WORD_CHAR = '#'; // char prefix to word nG from vocab
        const char FIELD_NAME_CHAR = '&'; // char prefix to field name 
        const int MIN_WORD_LENGTH = 3;
        const string DEFAULT_PATH = ".\\index\\"; // default path to store index files
        const float DOC_RANK_MULTIPLY = 10; // Rank multiplier when found in headers


        /* Index constructor
         *   path - working folder, if set it will load index
         *            autmatically, else set WorkPath before call to Load or CreateAsync
         *   InMemory - if true load into memory, else on disk         
         *   vocs - vocabs to use with index 
         */
        public Index(string path=null, bool InMemory=false, Vocab []vocs=null)
        {
          
            MaxDegreeOfParallelism = 2;
            MaxTmpIndexItems = MAX_DEF_TMP_INDEXITEMS;
            if (path != null)
                WorkPath = path;
            else WorkPath = DEFAULT_PATH;

            if (path !=null)
                Load(path);
            
            this.InMemory = InMemory;

            if (vocs!=null)
             this.vocs = new List<Vocab>(vocs);
        }

        SortedList<string, IndexSequence> self = new SortedList<string, IndexSequence>();

        public bool InMemory { get; private set; } = false; /* Load into memory or leave on disk */
        public bool bKeepForms { get; set; } = true;/* Keep full forms of the words */
        public bool CanSearch { get; private set; } = false; /* If index is loaded */

        ///  Stemmers table for string name
        /*
        public static (string lang, Type type, string letters)[] Stemmers = {
                ("ru",typeof (RussianStemmer),"А-Яа-яЁё"),
                ("en",typeof (EnglishStemmer),"A-Za-z"),
                ("us",typeof (EnglishStemmer),"A-Za-z"),
                ("de",typeof (GermanStemmer),"A-Za-zẞßäüöÄÖÜ"),
                ("fr",typeof (FrenchStemmer),"A-Za-zÉéÂâÀàÊêÈèËëÇçÎîÏïÔôÛûÙùÜüŸÿ")
            };*/
        public static (string lang, Type type)[] Stemmers = {
                ("ru",typeof (RussianStemmer)),
                ("en",typeof (EnglishStemmer)),
                ("de",typeof (GermanStemmer)),
                ("fr",typeof (FrenchStemmer)),
                ("sp",typeof (SpanishStemmer)),
                ("it",typeof (ItalianStemmer))
            };


        public class SearchResult
        {
            public HashSet<ResultDocument> foundDocs = new HashSet<ResultDocument>();
            public List<ResultDocPage> foundPages = new List<ResultDocPage>();
            public bool Success = true;
            public string Error { get; protected set; } ="";
            public override bool Equals(object obj)
            {
                if (obj.GetType()==typeof(SearchResult))
                {
                    SearchResult res = (SearchResult)obj;
                    
                    return (res.foundPages.SequenceEqual(foundPages));
                }
                return base.Equals(obj);
            }
        }

        public class ErrorSearchResult: SearchResult
        {
            public ErrorSearchResult(string error) : base()
            {
                Success = false;
                Error = error;
            }
        }

        /* Search result documet  class */
        public class ResultDocument: Document
        {
            public ResultDocument() : base() { }
            public ResultDocument(Document d) : base(d.Name,d.nPages) { }
            public ResultDocument(string s, int n=0) : base(s,n) { }
            public HashSet<ResultDocPage> pages=new HashSet<ResultDocPage>();
            public Dictionary<string, string> headers;
            public float rank;
            public override bool Equals(object obj)
            {
                if (obj.GetType() != GetType()) return false;

                return (((Document)obj).Name.Equals(Name));
            }
            public override int GetHashCode()
            {
                return Name.GetHashCode();
            }
            public void MakeHeaders(string str)
            {
                headers = new Dictionary<string, string>();
                string[] splits = str.Split(new char[] { '=', '\n' });
                for (int q = 0; q < splits.Length-1; q += 2)
                {
                    headers.Add(splits[q], splits[q + 1]);
                }
            }

        }
        /* Search result documet page class */
        public class ResultDocPage 
        {
            public ResultDocPage(DocPage p)
            {
                id = p.id;
            }
            public string id;
            public float rank;
            public List<int> pos = new List<int>(); // positions on the page
            public string text; //surrounding text
            public override bool Equals(object obj)
            {
                if (obj.GetType().Equals(typeof(ResultDocPage)))
                {
                    ResultDocPage page = (ResultDocPage)obj;
                    return (id == page.id) && (pos.SequenceEqual(page.pos));
                }
                return base.Equals(obj);
            }
        }

        private static string FromInt(int i) { return (KNOWN_WORD_CHAR + String.Format("{0:X}", i)); } // Convert.ToBase64String(new byte[] { (byte)((i >> 24) & 0xFF), (byte)((i >> 16) & 0xFF), (byte)((i >> 8) & 0xFF), (byte)((i) & 0xFF) }).TrimEnd('=')); }

        private IndexSequence LoadSequence(IndexSequence seq)
        {
            if (reader != null)
            {
                IEnumerator<ulong> e = seq.GetEnumerator();
                e.MoveNext();
                int len = (int) e.Current;
                e.MoveNext();
                long off = (long) e.Current - len;
                reader.BaseStream.Seek(off, SeekOrigin.Begin);
                byte[] buffer = new byte[len * 2];
                reader.BaseStream.Read(buffer, 0, len * 2);
                ushort[] arr = new ushort[len];
                Buffer.BlockCopy(buffer, 0, arr, 0, len*2);
                return (new IndexSequence(arr));

            }
            return (null);
        }

        private StreamReader reader = null;
        protected List<Vocab> vocs = new List<Vocab>();

        public void AddVoc (Vocab voc)
        {
            vocs.Add(voc);
        }

        public HashSet<string> stopWords = new HashSet<string>();
        public void LoadStopWords(string file)
        {
            stopWords = new HashSet<string>((from string s in File.ReadAllLines(file) where ((s.Trim(' ').Length > 0) && (!s.Contains(';'))) select s).ToList());
        }
        public void AddStopWords(string [] words)
        {
            foreach (string w in words) stopWords.Add(w);

        }

        private void Close()
        {
            CanSearch = false;

            if (reader != null)
                reader.Close();
            reader = null;

        }

        ~Index()
        {
            Close();

        }
        public int Count { get => self.Count; }

        public IndexSequence this[string key]
        {
           get {
            IndexSequence seq = self[key];
                if (!InMemory)
                    seq = LoadSequence(seq);
            return (seq);
            }
        }
        private IndexSequence SearchField(string field, string value)
        {
            try
            {
              IndexSequence seq = this[FIELD_NAME_CHAR + field.ToLower()];
              IndexSequence newseq = SearchWord(value.ToLower());
              seq.R = -1;
              return (seq*newseq);
            }
            catch (Exception e) {
                /* NOT FOUND */
            }

            return new IndexSequence();
/*
            var _matches = Regex.Matches(value, @"\b\w+\b");
            IndexSequence orVals = new IndexSequence();
                IndexSequence seq = this[FIELD_NAME_CHAR + field.ToLower()];
                foreach (var match in _matches)
                {
                    IndexSequence newseq = SearchWord(match.ToString().ToLower());
                    newseq.R = -1;
                    newseq *= seq;
                    orVals += newseq;
                }

            return (orVals);
            */
        }

        private IndexSequence SearchWord(string word)
        {
            if (word.Length < MIN_WORD_LENGTH) return (new IndexSequence(/* Empty */));
            string stemmed = word;
            

            IndexSequence res=null;
            try
            {
                int nG = 0;
                int nVoc = 0;
                foreach (Vocab voc in vocs)
                {
                    // TODO: detect prefered language
                    if ((voc != null) && (word[0] >= voc.Range.begin) && (word[0] <= voc.Range.end))
                    {
                        stemmed = voc.Stem(word);
                        nG = voc.Search(stemmed);
                        if (((nG & Vocab.GROUP_NOT_EXCACT_WORD_MASK) != 0) && (stemmed.Equals(word))) nG = 0;
                        if (nG != 0) break;
                    }
                    nVoc++;
                }
                if (nG != 0)
                {
                    string str = FromInt((nVoc<<24) | (nG & Vocab.GROUP_NUMBER_MASK));
                    return (this[str]);

                }
                else
                {
                    word = WORD_BEGIN_CHAR + word + WORD_END_CHAR;

                    for (int q = 0; q < word.Length; q += SUBWORD_LENGTH)
                    {
                        if (res == null) res = this[word.Substring(q, Math.Min(word.Length - q, SUBWORD_LENGTH))];
                        else res *= this[word.Substring(q, Math.Min(word.Length - q, SUBWORD_LENGTH))];
                        res.R = -(q + 1);
                    }
                    IndexSequence.Builder newres = new IndexSequence.Builder(Math.Abs(res.R));
                    newres.AddRange(res);
/*                   
                    if (res.MinCount > 1)
                    {
                        ulong prevc = res[0];
                        newres.Add(prevc);
                        foreach (ulong c in res)
                        {
                            if (c - prevc > (ulong)Math.Abs(res.R)) newres.Add(c);
                            prevc = c;
                        }
                    }*/
                    return (newres.build());
                }

            }
            catch (Exception e)
            {
                res = new IndexSequence(/* Empty */);
            }
            return (res);
        }

        public class SearchOptions
        {
            public int dist; // max distance between words in letters

        }
        private string PrepareRequest(string req,out string []_fields, bool keepshort = false)
        {
            List<string> fields = new List<string>();
            /*  foreach (Match match in Regex.Matches(req, @"{*(\w+)[ ]*=(.+)}"))
              {
                  fields.Append($"_GEF(\"{match.Groups[1]}={match.Groups[2]}\") * ");
                  //" GetField(\"${1}\",\"${2}\") * ");
              }*/
            req = Regex.Replace(req, @"[^\w(){}=|]|_+", " "); // delete incorrect symbols

            req = Regex.Replace(req, @"{*(\w+)[ ]*=([\w|() ]+)}", (Match match) => {
                string[] arr;
                string l = PrepareRequest(match.Groups[2].ToString(), out arr, true); // like _GET("A")
                l = Regex.Replace(l, @"_Get\(\""(\w+)\""\)", "_Getf(\""+match.Groups[1].ToString()+"=${1}\")");
                fields.Add(l);
                return "";// $"___{fields.Count-1}";
            });

            _fields = fields.ToArray();

            if (!keepshort)
             req = Regex.Replace(req, @"\b\w{1,2}\b", " "); //delete words with 1,2 letters

            foreach (string st in stopWords) req = Regex.Replace(req, $@"\b{st}\b", ""); //delete stopwords
                                                                                         //req = Regex.Replace(req, @"\b(^\w|[^(+)])+\b", " ");
            req = Regex.Replace(req, @"\|", "+"); // replace OR operator

            //req = Regex.Replace(req, @"\b(\w+)(\s*\|\s*(\w+)\b)+", "(${0})");
            //req = Regex.Replace(req, @"\b(\w+)\s*\|\s*(\w+)\b", "_Get(\"${1}\") + _Get(\"${2}\") ");
            //req = Regex.Replace(req, @"\|\s*(\w+)\b", " + _Get(\"${0}\")");

            req = Regex.Replace(req, @"\b\s+\b", "*");
            req = Regex.Replace(req, @"\)\s+\b", ")*");
            req = Regex.Replace(req, @"\b\s+\(", "*(");

            req = Regex.Replace(req, @"\b(?!___)(\w+)\b", "_Get(\"${0}\")");

            //req = Regex.Replace(req, @"(?<!_GET\(\"")\b(?!_)\w +\b", "_Get(\"${0}\")");



            //req = Regex.Replace(req, @"\b(\w+)\W*\+\W*(\w+)\b", "_Get(\"${1}\") + _Get(\"${2}\") ");
            //req = Regex.Replace(req, @"\b(\w+)\s+(\w+)\b", "_Get(\"${1}\") * _Get(\"${2}\") ");
            //req = Regex.Replace(req, @"\B\W*\+", " +");

            //req += fields.ToString();
            int q = 0;
            foreach (string item in fields){
                req = req.Replace($"___{q++}", $"({item})");
            }

            req = req.TrimEnd(new char[] { ' ', '*' });

            return req;
        }

        protected SearchResult PrepareSearchResult(IndexSequence res,string [] filter)
        {
            SearchResult result = new SearchResult();
            DocPage prevp = null;
            Document prevd = null;
            ResultDocPage lastResultDocPage = null;
            ResultDocument lastDoc = null;
            foreach (ulong coord in res)
            {

                DocPage _p = PagesList.GetPage(coord);
                if (!_p.Equals(prevp))
                {
                    if (lastResultDocPage != null)
                        lastResultDocPage.rank = 1 + (float)Math.Log(lastResultDocPage.pos.Count());
                    lastResultDocPage = new ResultDocPage(_p);
                    lastResultDocPage.pos.Add((int)(coord - _p.coord));
                    result.foundPages.Add(lastResultDocPage);
                    if (prevd != _p.doc)
                    {
                        ResultDocument doc = new ResultDocument(_p.doc);
                        // check filter
                        if (result.foundDocs.Count < MAX_FOUND_DOCS)
                        {
                            bool matched = filter.Length == 0;
                            foreach (string filt in filter)
                            {
                                if (Regex.Match(doc.Name, filt).Success) { matched = true; break; }
                            }
                            if (matched)
                                result.foundDocs.Add(doc);
                        }
                        lastDoc = doc;
                    }
                    //lastResultDocPage.doc = lastDoc;

                    lastDoc.pages.Add(lastResultDocPage);
                    lastDoc.rank += lastResultDocPage.rank;
                    prevp = _p;
                    prevd = _p.doc;
                }
                else
                    lastResultDocPage.pos.Add((int)(coord - _p.coord));

                if (result.foundPages.Count > MAX_FOUND_PAGES) break;

            }
            // retrieve surrounding text

            if (lastResultDocPage != null)
                lastResultDocPage.rank = 1 + (float)Math.Log(lastResultDocPage.pos.Count());

            return result;
        }

        /* Returns SearchResult var with documents that are in both res1 and res2 */
        protected SearchResult CombineSearchResults(SearchResult res1,SearchResult res2)
        {
            res1.foundDocs.IntersectWith(res2.foundDocs);

            return res1;
        }

        public SearchResult Search(string req, SearchOptions opt=null)
        {

            //using System.Linq.Dynamic, 
            //            Expression expr = System.Linq.Dynamic.DynamicExpression.Parse(typeof(IndexSequence), "GetTest(1) * GetTest(2)", null);
            //            LambdaExpression e = Expression.Lambda(expr);
            //            IndexSequence tst = ((Func<IndexSequence>)e.Compile())();

            if (!CanSearch) return (new ErrorSearchResult("Index is not built"));
            try
            {
                lock (DoSearchLock) // wait antil can search
                {

                    var interpreter = new Interpreter();

                    req = req.ToLower();

                    /* search filter */
                    List<string> filter = new List<string>();
                    var matches = Regex.Match(req, @"\B-filter:([\w\*\?\\.()+{}/]+,?)+");
                    if (matches.Groups.Count > 1) {
                        foreach (var cap in matches.Groups[1].Captures)
                        {

                            filter.Add(cap.ToString().Trim(','));
                        }
                    }
                    req = Regex.Replace(req, @"\B-filter:([\w\*\?\\.()+{}/]+,?)+", " ");

                    string[] fields;
                    req = PrepareRequest(req,out fields);

                    int R = 255;
                    if (opt != null) R = opt.dist;

                    Func<string, IndexSequence> Get = (word) => { IndexSequence seq = SearchWord(word); seq.R = Math.Sign(R) * (word.Length + Math.Abs(R)); return (seq); };
                    Func<string, IndexSequence> GetField = (namevalue) => {
                        // Search by field name=value using OR combinatin operator for value subwords
                        string name = namevalue.Split('=')[0];
                        string value = namevalue.Split('=')[1];
                        return (SearchField(name, value));
                    };
                    interpreter.SetFunction("_Get", Get);
                    interpreter.SetFunction("_Getf", GetField);

                    IndexSequence res=null;
                    IndexSequence resf = null;
                    if (req.Length > 0)
                    {
                        try
                        {
                            res = interpreter.Eval<IndexSequence>(req);
                        }
                        catch (DynamicExpresso.Exceptions.DynamicExpressoException e)
                        {
                            string s = "Syntax Error in search request";
                            Console.WriteLine(s);
                            return new ErrorSearchResult(s);
                        }
                    }
                    if (fields.Length > 0)
                    {
                        try
                        {
                            resf = interpreter.Eval<IndexSequence>(fields.Aggregate((s1,s2)=> { return s1 + " "+ s2; }));
                        }
                        catch (DynamicExpresso.Exceptions.DynamicExpressoException e)
                        {
                            string s = "Syntax Error in search request";
                            Console.WriteLine(s);
                            return new ErrorSearchResult(s);
                        }
                    }
                    if (res == null) res = resf;
                    if (res != null)
                    {


                        SearchResult result = PrepareSearchResult(res,filter.ToArray());
                        if (resf!=null)
                        {
                            result = CombineSearchResults(result,PrepareSearchResult(resf, new string[] { }));
                        }


                            
                      
                        foreach (var doc in result.foundDocs)
                        {
                            doc.rank = doc.pages.Sum((page) => { return page.rank; });                            
                            doc.rank = 1 + (float) Math.Log(doc.rank);
                            if (doc.pages.First().id.Equals("0"))
                                 doc.rank *= DOC_RANK_MULTIPLY;

                            foreach (var source in sources)
                             if (source.Name.Equals(doc.Name.Split(':')[0])) 
                                {
                                    if ((source!=null) && (source is IIndexDirectDataSource))
                                    {
                                        IIndexDirectDocument document = (source as IIndexDirectDataSource)[doc.Name.Substring(doc.Name.Split(':')[0].Length + 1)];
                                        if (document != null)
                                        {
                                            string headers = document["0"].text;
                                            if (doc.pages.First().id.Equals("0"))
                                            {
                                                // TODO:  Select words in text
                                                /* For example */
                                                headers = headers.Insert(doc.pages.First().pos[0], "->");
                                            }
                                            doc.MakeHeaders(headers);
                                            doc.pages.RemoveWhere((p) => { return p.id.Equals("0"); });
                                            foreach (var page in doc.pages)
                                            {
                                                string text = document[page.id].text;
                                                // TODO: Select words
                                                int[] Range = { 0, 0 };
                                                Range[0] = Math.Min(Math.Max(0, page.pos.Min() - 64), text.Length);
                                                Range[1] = Math.Min(Math.Min(page.pos.Max() + 64, text.Length), Range[0] + 256);

                                                page.text = PreparePageText(text.Substring(Range[0], Range[1] - Range[0]));


                                            }
                                            document.Dispose();
                                        }
                                    }
                                    break;
                                }
                            
                        }

                        result.foundDocs = new HashSet<ResultDocument>((from doc in result.foundDocs orderby doc.rank select doc).ToArray());

                        return result;

                    }
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return new ErrorSearchResult("Error: " + e.Message);
            }

            Console.WriteLine("Not fond!");
            return (new SearchResult());
        }
        private static string PreparePageText(string text)
        {
            text = Regex.Replace(text, @"\b\W*\.+\W*\b", ". ");
            text = Regex.Replace(text, @"\b\W*\?+\W*\b", "? ");
            text = Regex.Replace(text, @"\b\W*!+\W*\b", "! ");
            text = Regex.Replace(text, @"\b\W*:+\W*\b", ": ");
            text = Regex.Replace(text, @"\b\W*,+\W*\b", ", ");
            text = text.Replace("\r", " ");
            text = text.Replace("\n", " ");
            return (text);

        }

        // get most popular words
        public Dictionary<string, int> Histogram()
        {
            Dictionary<string, int> dict = new Dictionary<string, int>();
            try
            {
                foreach (var pair in (from item in self let v = (InMemory ? item.Value.Count : (int)item.Value.First()) orderby -v select new KeyValuePair<string, int>(item.Key, (int)v)).Take(1000))
                {
                    if (pair.Key[0] == KNOWN_WORD_CHAR)
                    {
                        int p = int.Parse(pair.Key.Substring(1), System.Globalization.NumberStyles.AllowHexSpecifier);
                        int nV = (p >> 24);
                        
                        dict.Add("(" + KNOWN_WORD_CHAR + (from ii in vocs[nV] where (ii.Value == (p & 0xFFFFFF)) select ii).Take(10).Select((i)=>i.Key).Aggregate((a,b)=> { return (a + "," + b); })+")",
                            pair.Value);
                    }
                    else
                    dict.Add(pair.Key, pair.Value);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Histogram: " + e.Message);
            }
            return (dict);
        }

        public bool Load(string path)
        {
            if ((File.Exists(path + "\\" + ".index")) && (File.Exists(path + "\\" + ".index.list")))
            {
                CanSearch = false;
                self.Clear();

                try
                {
                    BinaryReader bin = new BinaryReader(File.OpenRead(path + "\\" + ".index"));


                    try
                    {
                        bin.ReadUInt64();
                        do
                        {
                            string s = bin.ReadString();
                            

                           
                            //if (n > 0)
                            //{
                                if (InMemory)
                                {

                                    /*ushort[] arr = new ushort[n];
                                    byte[] bytes = bin.ReadBytes(n * sizeof(int));
                                    Buffer.BlockCopy(bytes, 0, arr, 0, n * sizeof(ushort));
                                    */
                                    IndexSequence seq = new IndexSequence();
                                    seq.Read(bin);
                                    self.Add(s, seq);
                                }
                                else
                                {
                                    int n = bin.ReadInt32();
                                    self.Add(s, new IndexSequence.Builder().Add((ulong)n).Add((ulong)(bin.BaseStream.Position+n)).build());
                                    bin.BaseStream.Seek(n * IndexSequence.DataUnitSize, SeekOrigin.Current);
                                }
                            //}
                            //else throw new InvalidDataException();

                        }
                        while (true);
                    }
                    catch (EndOfStreamException e)
                    {

                    }

                    if (!InMemory) { reader = new StreamReader(bin.BaseStream); }
                    else bin.Close();

                    bin = new BinaryReader(File.OpenRead(path + "\\" + ".index.list"));

                    PagesList = new IndexPageList();
                    PagesList.Load(bin);
                    bin.Close();

                    CanSearch = true;
                    return (true);

                }
                catch (Exception e)
                {

                }
            }
            return (false);
        }

        public void Cancel()
        {
            cancelationToken.Cancel();
        }


        private ParallelOptions po = new ParallelOptions();
        public CancellationTokenSource cancelationToken { get; private set; }
        public int MaxDegreeOfParallelism { get => po.MaxDegreeOfParallelism; set => po.MaxDegreeOfParallelism = value; }

        public enum Status
        {
            Idle, Nav, Index, Merge
        };

        public Status status { get; private set; }
        public bool IsCreating { get => status != Status.Idle; }
        private object DoSearchLock = new object();

        private IIndexDataSource[] sources;
        public int nDataSources { get => sources!=null?sources.Length:0; }
        public bool CanIndex { get => (nDataSources > 0); }

        const string CACHE_END = ".cache.zip";
        public void AddDataSource(IIndexDataSource source)
        {
            source = new IndexTextCacheDataSource(source, WorkPath + "\\" + source.Name + CACHE_END);
            if (sources == null) { sources = new IIndexDataSource[1]; sources[0] = source; }
            else
            {
             Array.Resize(ref sources, sources.Length + 1);
             sources[sources.Length - 1] = source;
            }
            
        }

        event EventHandler CreationDone;


      
        public async Task CreateAsync()
        {
            if ((sources == null) || (sources.Length == 0) || (sources[0] == null)) { if (CreationDone!=null) CreationDone(this,new EventArgs()); return; }// Task.FromException(new Exception("No data sources")); }

            if (status == Status.Idle)
            { //first time
                //Close();
                long startTime = Environment.TickCount;

                status = Status.Nav;
                cancelationToken  = new CancellationTokenSource();
                po.CancellationToken = cancelationToken.Token;

                await (Task.Run(async () =>
                {
                try
                {

                    if (Directory.Exists(WorkPath))
                        foreach (string dir in Directory.GetDirectories(WorkPath))
                            Directory.Delete(dir, true);
                    else
                        Directory.CreateDirectory(WorkPath);


                    List<Task> tasks = new List<Task>();


                        List<IIndexDataSource> tmpSources = new List<IIndexDataSource>();
                    foreach (IIndexDataSource source in sources)
                    {
                        IIndexDataSource tmpsource = source;
                        if (source.GetType().Equals(typeof(IndexTextCacheDataSource)))
                        {
                            tmpsource = new IndexTextCacheDataSource(((IndexTextCacheDataSource)source).source, WorkPath + "\\" + source.Name + CACHE_END + "_");
                            tmpSources.Add(tmpsource);

                        }

                        tmpsource.Reset();
                        for (int q = 0; q < Math.Max(1, MaxDegreeOfParallelism); q++)
                            tasks.Add(Task.Factory.StartNew(() => { IndexTask(tmpsource);}, po.CancellationToken));
                    }
                    
                    status = Status.Index;
                    await Task.WhenAll(tasks.ToArray());//, po.CancellationToken);
                    Console.WriteLine("Index finished");

                    // dispose temporaty sources
                    foreach (IIndexDataSource source in tmpSources)
                       source.Dispose();

                    // Next parallel Merge 
                    status = Status.Merge;
                    Directory.GetDirectories(WorkPath).AsParallel().ForAll((i) => MergeAll(i));
                    // Overall Merge
                        Console.WriteLine("Final merge...");
                    ArrayList files = new ArrayList();
                    foreach (string dir in Directory.GetDirectories(WorkPath))
                    {
                        string[] f = Directory.GetFiles(dir, "*.tmpind");
                        if (f.Length > 0) files.Add(f[0]);
                    }

                    MergeIndexes((String[])files.ToArray(typeof(string)), WorkPath + "\\" + ".index");
         
                     //CanSearch now false   

                        // exchanging datasources
                        lock (DoSearchLock)
                        {
                            List<IIndexDataSource> ds = new List<IIndexDataSource>();
                            foreach (IIndexDataSource source in sources)
                            {
                                if (source.GetType().Equals(typeof(IndexTextCacheDataSource)))
                                {
                                    source.Dispose(); // free resources
                                    File.Delete(WorkPath + "\\" + source.Name + CACHE_END);
                                    File.Move(WorkPath + "\\" + source.Name + CACHE_END + "_", WorkPath + "\\" + source.Name + CACHE_END);
                                    ds.Add(new IndexTextCacheDataSource(((IndexTextCacheDataSource)source).source, WorkPath + "\\" + source.Name + CACHE_END));
                                }
                                else ds.Add(source);


                            }
                            sources = ds.ToArray();
                        }

                        Load(WorkPath);
                        CanSearch = true;

                        Array.ForEach(Directory.GetDirectories(WorkPath), i => Directory.Delete(i, true));

                        status = Status.Idle;

                        Console.WriteLine("Time elasped: {0} s", (Environment.TickCount - startTime) / 1000);
                    }
                    catch (Exception e)
                    {
                      Console.WriteLine("Error: "+e.Message);
                      status = Status.Idle;
                      CanSearch = false;
                       
                    }
                    cancelationToken = null;
                }, cancelationToken.Token));

            }
 
           if (CreationDone!=null) CreationDone(this,new EventArgs());
        }

        /* Merge all files in subfolders */
        private void MergeAll(string Path)
        {
            try
            {
                string[] files;
                do
                {

                    files = Directory.GetFiles(Path, "*.tmpind");
                    if (files.Length > 1)
                    {
                        int nMax = 5;
                        for (int q = 0; q < files.Length - 1; q += nMax)
                        {
                            string[] newfiles = new string[Math.Min(nMax, files.Length - q)];
                            Array.Copy(files, q, newfiles, 0, newfiles.Length);
                            MergeFiles(newfiles, newfiles[0] + "_");
                            Console.WriteLine("Merge: " + newfiles[0] + " + " + newfiles[1]);
                            foreach (string f in newfiles) File.Delete(f);
                            File.Move(newfiles[0] + "_", newfiles[0]);
                            File.Delete(newfiles[0] + "_");

                        }

                    }

                } while (files.Length > 1);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error merging files: ", e.Message);
                this.Cancel();

            }
            // now we have 1 .ind file for each folder

        }

        /* Merges tmpindex in files together */
        /* output to fname */
        private void MergeFiles(string[] files, string fname, bool shift_coords = false)
        {
            try
            {

                BinaryReader[] bins = new BinaryReader[files.Length];

                ulong[] maxCoords = new ulong[files.Length];
                ulong[] shifts = new ulong[files.Length];
                shifts[0] = 0;// first index have always 0 shift
                for (int q = 0; q < files.Length; q++)
                {
                    bins[q] = new BinaryReader(File.OpenRead(files[q]));

                    maxCoords[q] = bins[q].ReadUInt64();
                    if (q > 0) shifts[q] = shifts[q - 1] + maxCoords[q - 1];
                }

                BinaryWriter wr = new BinaryWriter(File.Create(fname));
                wr.Write(maxCoords.Max());


                string[] s = new string[files.Length]; // next words
                IndexSequence[] arr = new IndexSequence[files.Length]; // coord arrays

                for (int q = 0; q < s.Length; q++) {
                    s[q] = " ";
                    arr[q] = new IndexSequence();
                }
                
                //int[] n = new int[files.Length]; // numbers of coords in vectors
                bool[] readnext = new bool[files.Length]; // what index read next
                //Array.Fill(readnext, true); // need to read from each index first
                for (int q = 0; q < readnext.Length; q++) readnext[q] = true;

                var s_end = from str in s where str.Length != 0 select str;
                
                do
                {
                    for (int q = 0; q < files.Length; q++)
                        if ((readnext[q]) && (s[q].Length > 0))// read next string & array if previous was read
                        {
                            try
                            {
                                s[q] = bins[q].ReadString();
                                arr[q].Read(bins[q]);

                            }
                            catch (EndOfStreamException e)
                            {
                                s[q] = "";
                            }

                        }
                    // define next step
                    List<KeyValuePair<string, int>> list = new List<KeyValuePair<string, int>>();
                    for (int q = 0; q < files.Length; q++)
                        if (s[q].Length > 0) // was previously read
                            list.Add(new KeyValuePair<string, int>(s[q], q));
                    list.Sort(new Comparison<KeyValuePair<string, int>>((a, b) =>
                    {
                    // compare by words and then by coordinates
                    int i = a.Key.CompareTo(b.Key);
                        if (i == 0)
                        {
                            if (shift_coords) return (maxCoords[a.Value].CompareTo(maxCoords[b.Value]));
                            else return (shifts[a.Value].CompareTo(shifts[b.Value]));
                        }
                        return (i);
                    }));
                    //Array.Fill(readnext, false); // .Net Core method
                    for (int q = 0; q < readnext.Length; q++)
                        readnext[q] = false;

                    if (list.Count > 0)
                    {
                        readnext[list[0].Value] = true;
                        //int nsize = n[list[0].Value];

                        for (int q = 1; q < list.Count; q++)
                            if (list[q].Key.Equals(list[q - 1].Key))
                            {
                                readnext[list[q].Value] = true;
                                //nsize += n[list[q].Value];
                            }

                        wr.Write(s[list[0].Value]);
                        //wr.Write(nsize);
                        IndexSequence.Builder bldr = new IndexSequence.Builder();
                        for (int q = 0; q < files.Length; q++)
                            if (readnext[q])
                            {
                                // write coord array
                          
                                if (shift_coords) { arr[q].Shift(shifts[q]); } //for (int w = 0; w < arr[q].Length; w++) arr[q][w] += (uint)(shifts[q]); }
                                bldr.AddRange(arr[q]);
                            }
                        bldr.build().Write(wr);
                    }

                } while (s_end.Count() > 0);


                foreach (BinaryReader bin in bins) bin.Close();
                wr.Close();

            }
            catch (Exception e)
            {
                Console.WriteLine($"Error merging: " + e.Message);
            }

        }

        /* Merge different indexes and file lists => one index*/
        /* files[i] is path to .tmpind file,
         * correcponing .list file present
        /* Result index is stored in one file output*/
        void MergeIndexes(string[] files, string output)
        {
            if (files.Length == 0) return;
            // merge in temp file first
            MergeFiles(files, output+"_", true); 
            PagesList = new IndexPageList();

            // merge lists
            try
            {
                ulong[] shifts = new ulong[files.Length];
                shifts[0] = 0;
                //            Dictionary<string, int> result = new Dictionary<string, int>();
                ulong m = 0;
                for (int q = 0; q < files.Length; q++)
                {
                    if (q > 0) shifts[q] = shifts[q - 1] + m;
                    BinaryReader bin = new BinaryReader(File.OpenRead(files[q]));
                    m = bin.ReadUInt64();
                    bin.Close();

                    BinaryFormatter binf = new BinaryFormatter();
                    FileStream f = File.OpenRead(new FileInfo(files[q]).DirectoryName + "\\index.tmplist");

                    List<KeyValuePair<string, ulong>> dict = (List<KeyValuePair<string, ulong>>)binf.Deserialize(f);
                    f.Close();


                    Document doc = new Document();

                    foreach (KeyValuePair<string, ulong> d in dict)
                    {
                        if (d.Key[0] != ':') // doc
                        {
                            doc = new Document(d.Key);
                            PagesList.AddDocument(doc);
                        }
                        else
                        {
                            PagesList.Add(d.Value+shifts[q], new DocPage(doc, d.Key.Substring(1)));
                        }


                    }
                }

                lock (DoSearchLock) // stop all seach tasks before replacing index
                {
                    CanSearch = false;
                }
                    if (File.Exists(output + ".list")) File.Delete(output + ".list");
                    BinaryWriter binOut = new BinaryWriter(File.Create(output + ".list"));
                    PagesList.Save(binOut);
                    binOut.Close();

                    
                    Close();
                    if (File.Exists(output)) File.Delete(output);
                    File.Move(output + "_", output);
                


                foreach (string file in files) File.Delete(file);


            }
            catch (Exception e)
            {
                Console.WriteLine("Error mering indexes: " + e.Message);
            }

            if (File.Exists(output + "_"))
             File.Delete(output+"_");
        }

       

        protected virtual bool IsLetter(char l) 
         {   
             /*
             if ((l >= 'a') && (l <= 'z')) return (true);
             if ((l >= 'а') && (l <= 'я')) return (true);
             if ((l >= '0') && (l <= '9')) return (true);
             if ((l >= 'A') && (l <= 'Z')) return (true);
             if ((l >= 'А') && (l <= 'Я')) return (true);
             if ((l == 'ё') || (l == 'Ё')) return (true);
             */
             if (l == '_') return (false);
             return Regex.Match("" + l, @"\w").Success;
         }



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

        class IndexPageList : Dictionary<ulong, DocPage>
        {
            List<Document> DocsList = new List<Document>();
            public void AddDocument(Document doc)
            {
                DocsList.Add(doc);
            }

            IEnumerator<KeyValuePair<ulong, DocPage>> _enum;

            ulong prevc = 0xFFFFFFFF;

            public DocPage GetPage(ulong coord)
            {


                if (prevc == 0xFFFFFFFF) { _enum = this.AsEnumerable().GetEnumerator(); _enum.MoveNext(); prevc = 0; }
                else
                {
                    if (_enum.Current.Key == coord) return (_enum.Current.Value);
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
                        bin.Write(":" + p.Value.id);
                    }
                    else
                    {
                        bin.Write(":" + p.Value.id);
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
                        if (s[0] != ':')
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

        /* Thread Safe temporary index class */
        /* Temporary indexes then merged into final index */
        class TempIndex : SortedList<string, IndexSequence.Builder>
        {
            public int nTmpIndex = 0;


            public TempIndex(string tmppath, int _MaxItems) : base()
            {
                this.MaxItems = _MaxItems;
                this.Path = tmppath;
                TotalCount = 0;
                maxCoord = 0;
            }
            public int MaxItems { get; }
            public string Path { get; }
            public int TotalCount { get; private set; }
            public ulong maxCoord { get; private set; } // maximum coordinate

            /* Add word into TemIndex
             * coord must be greater with each call */
            public void Add(string word, ulong coord)
            {
                maxCoord = coord; // coord increases with each call
                IndexSequence.Builder val;
                if (!base.TryGetValue(word, out val))
                {
                    val = new IndexSequence.Builder();
                    base.Add(word, val);
                }
                val.Add(coord);
                TotalCount++;
                if (TotalCount > MaxItems)
                {
                    Save();
                    Clear();
                    TotalCount = 0;
                }
            }

            public void Save()
            {
                try
                {
                    Interlocked.Increment(ref nTmpIndex);
                    FileStream file = File.Create($"{Path}\\{nTmpIndex}.tmpind");
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
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error saving TempIndex: " + e.Message);
                }

            }


        };

        public string WorkPath;


      
        private void IndexTask(IIndexDataSource source)
        {
           

            try
            {
                // main index task
                Directory.CreateDirectory(WorkPath + "\\" + Task.CurrentId);
                TempIndex index = new TempIndex(WorkPath + "\\" + Task.CurrentId, MaxTmpIndexItems);
                List<KeyValuePair<string, ulong>> files = new List<KeyValuePair<string, ulong>>();

                Console.WriteLine("Started IndexTask id{0}", Task.CurrentId);

                // task to process files

                
                ulong coord = 0;
                do
                {

                 //   if (cancel.Token.IsCancellationRequested) break;

                    IIndexDocument doc = source.Next();

                    if (doc == null) break;

                    Console.WriteLine("ID:{0} <-{1}", Task.CurrentId, doc.Name);
                    // TODO: File must fit RAM
                    files.Add(new KeyValuePair<string, ulong>(source.Name + ":" + doc.Name, index.maxCoord));
                    
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
                                                int dc = fields[0].Length+1;
                                                foreach (var match in matches)
                                                {
                                                    if ((match.Length >= 1) &&  (IsLetter(match[0]))) //&& (!stopWords.Contains(match)))
                                                    {
                                                        //coord += (ulong)(fields[0].Length);
                                                        index.Add(FIELD_NAME_CHAR + fields[0], (uint)(coord + (ulong)(dc-1)));
                                                        AddToTempIndex(match.ToLower(), coord + (ulong)dc, index);
                                                    }
                                                    dc += match.Length;
                                                }
                                            }
                                            coord += (ulong) line.Length+1; // +1 from \n
                                        }
                                    }
                                    catch (EndOfStreamException e)
                                    { }
                                }
                                files.Add(new KeyValuePair<string, ulong>(":" + page.id, coord));

                                continue;
                            }
                            //foreach (string ss in System.Text.RegularExpressions.Regex.Split(c, "\\b"))
                            string ss = "";
                            ss += c[0];
                            for (int qq = 1; qq < c.Length; qq++)
                            {
                                if ((qq == c.Length - 1) || (IsLetter(c[qq]) != IsLetter(c[qq - 1])))
                                {
                                    // end of group

                                    if (ss.Length > 0)
                                    {
                                        if ((ss.Length >= MIN_WORD_LENGTH) && (IsLetter(ss[0])))
                                        {
                                            AddToTempIndex(ss, coord + (uint)(qq-ss.Length),  index);
                                        }

                                    }
                                    ss = "";
                                }
                                ss += c[qq];
                            }
                            coord += (uint)c.Length;
                            files.Add(new KeyValuePair<string, ulong>(":" + page.id, coord));
                   //         nextCoord.Add(coord,cancel.Token);
                     //       Console.WriteLine($"Task {Task.CurrentId} pushed next coord {coord}");
                            
                        }
                        catch (Exception e)
                        { 
                            Console.WriteLine("Error parsing file {0}: {1}", doc.Name, e.Message);
                        }

                    }

                    doc.Dispose();
                }
                while (true);

                index.Save();


                FileStream fileStream = File.Create($"{WorkPath + "\\" + Task.CurrentId}\\index.tmplist");
                BinaryFormatter bin = new BinaryFormatter();
                bin.Serialize(fileStream, files);
                fileStream.Close();


            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception in Task{Task.CurrentId}: " + e.Message);
            }


            Console.WriteLine($"Task{Task.CurrentId} exited");
        }

      
        // write word into temporary index 
        // ss - word, coord - current base coordinate, index - index to add 
        private void AddToTempIndex(string ss,ulong coord,  TempIndex index)
        {
            uint d = 0;
            string stemmed = ss;
            int nG = 0;
            int nVoc = 0;
            if (stopWords.Contains(ss)) return;

            foreach (Vocab voc in vocs)
            {
                if ((voc != null) && (ss[0] >= voc.Range.begin) && (ss[0] <= voc.Range.end) && ((stemmed = voc.Stem(ss)) != null) && ((nG = voc.Search(stemmed)) != 0))
                {
                    // writing group number rather then parts of the word

                    //if (((nG & Vocab.GROUP_NOT_EXCACT_WORD_MASK) != 0) && (stemmed.Equals(ss)))
                    //    nG = 0;
                    //else
                    {
                        string str = FromInt((nVoc << 24) | (nG & Vocab.GROUP_NUMBER_MASK));
                        //if (((nVoc << 24) | (nG & Vocab.GROUP_NUMBER_MASK)) == 0x2b8)
                       // {
                        //    break;
                       // }

                        index.Add(str, (uint)( coord));
                        if ((bKeepForms) && (stemmed.Length < ss.Length))
                        { // reminder
                            str = "";
                            if (ss.Length - stemmed.Length <= 2)
                                str = WORD_SUFFIX_CHAR + ss[0] + SUFFIX_DEVIDER_CHAR + ss.Substring(stemmed.Length);
                            else
                                str = WORD_SUFFIX_CHAR + ss.Substring(stemmed.Length);
                            index.Add(str, (uint)(coord + 1));

                        }
                    }
                    break;

                }
                nVoc++;
            }

            if (nG == 0)
            {
                string news = WORD_BEGIN_CHAR + ss + WORD_END_CHAR;
                for (int q = 0; q < news.Length; q += SUBWORD_LENGTH)
                {
                    index.Add(news.Substring(q, Math.Min(news.Length - q, SUBWORD_LENGTH)), (uint)(coord + d));
                    d++;
                }
            }

        }

        /* Call to free all resources used by index */
        public void Dispose()
        {
            CanSearch = false;
            foreach (IIndexDataSource source in sources)
              if (source.GetType()==typeof(IndexTextCacheDataSource))
            {
                    source.Dispose();
            }
            sources = new IIndexDataSource[] { };
            vocs.Clear();
            stopWords.Clear();
            self.Clear();
            if (reader!=null)
             reader.Dispose();
        }
    }
}