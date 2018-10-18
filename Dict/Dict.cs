using Iveonik.Stemmers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using System.Linq;


namespace Docodo
{

    /* Base class for loaded vocabulary */
    public class Vocab : Dictionary<string, int>
    {
        public const int GROUP_NOT_EXCACT_WORD_MASK = 0x01000000; // mask for bit in group number to skip the word if it's excact the same as its stemm
        public const int GROUP_NUMBER_MASK = 0xFFFFFF;

        public char[] Range = { 'a', 'z' }; // letters range of this vocab

        public void Load(string fname)
        {
            BinaryReader r = new BinaryReader(File.OpenRead(fname));
            try
            {
                Clear();
                do
                {
                    Add(r.ReadString(),r.ReadInt32());
                }
                while (true);

            }
            catch (EndOfStreamException e)
            {

            }
            finally
            {
                r.Close();
            }

            Range[0] = this.First().Key[0];
            Range[1] = this.Last().Key[0];
        }
        /* return word group or 0 if absent */
        public int Search(string word)
        {
            int nG = 0;
            TryGetValue(word, out nG);
            return (nG);
        }
    }

    /* Base class for building docodo vocabulary from custom morphological dictionaries*/
    /* Derive your own class and use AddWordsGroup method to add morphologicaly grouped words into voc*/
    /* then save result to the disk using build() method */
    public class VocBuilder : SortedDictionary<string, int>
    {
        public VocBuilder(IStemmer stemmer = null)
        {
            this.stemmer = stemmer;
        }
        public IStemmer stemmer { get; set; } = null;
        int nG=1;
        SortedDictionary<int, int> replaces = new SortedDictionary<int, int>();

        protected void AddWordsGroup(string[] grouplist)
        {
            int currNG = nG;//|Voc.GROUP_NOT_EXCACT_WORD_MASK;
                            // try change group id to the lowest one
            bool hasmatch = false; // has group stemm itsels
            bool found = false;
            HashSet<int> replaceGroups = new HashSet<int>();


            foreach (string word in grouplist)
            {
                string stemme = stemmer != null ? stemmer.Stem(word) : word;
                if ((!hasmatch) && (grouplist.Contains<string>(stemme)))
                    hasmatch = true;
                int newcurrNG = currNG;

                if (TryGetValue(stemme, out newcurrNG))
                {
                    // get real group
                    int inewcurrNG = 0;
                    if (replaces.TryGetValue(newcurrNG, out inewcurrNG))
                        newcurrNG = inewcurrNG;

                    if ((currNG & Vocab.GROUP_NUMBER_MASK) != (newcurrNG & Vocab.GROUP_NUMBER_MASK))
                    {
                        if (found)
                        {
                            //Console.WriteLine("Warging dublicate word group: {0} {1}", word, stemme);
                            // need to combine groups
                            replaceGroups.Add(newcurrNG & Vocab.GROUP_NUMBER_MASK);
                        }
                        else
                            currNG = newcurrNG;

                        found = true;
                    }
                }
            }


            // adding to vocab
            if ((currNG & Vocab.GROUP_NOT_EXCACT_WORD_MASK) == 0)
                hasmatch = true; // have matches in existing group 
            if (hasmatch) currNG &= ~Vocab.GROUP_NOT_EXCACT_WORD_MASK; // clear not-found bit

            foreach (int gr in replaceGroups)
                replaces.TryAdd(gr, currNG);


            foreach (string word in grouplist)
            {
                string stemme = stemmer != null ? stemmer.Stem(word) : word;

                int newcurrNG = currNG;

                if (!TryGetValue(stemme, out newcurrNG))
                    Add(stemme, currNG);
                else if ((hasmatch) && ((newcurrNG & Vocab.GROUP_NOT_EXCACT_WORD_MASK) != 0))
                {// replace nG flag to not set
                    this[stemme] = currNG & (~Vocab.GROUP_NOT_EXCACT_WORD_MASK);
                }

            }

            nG++;

        }

        public void build(string outfile)
        {
            BinaryWriter wr = new BinaryWriter(File.Create(outfile));
            foreach (KeyValuePair<string, int> pair in this)
            {
                wr.Write(pair.Key);
                int newnnG = 0;
                if (replaces.TryGetValue(pair.Value, out newnnG))
                    wr.Write(newnnG);
                else
                    wr.Write(pair.Value);

            }

            wr.Close();

        }
    }

    /* Create russian docodo vocab from OpenCorpora russian dictionary */
        class OpenCorporaVocBuilder: VocBuilder
    {
        public static void CreateFromOpenCorpora(String file,String outfile)
        {
            OpenCorporaVocBuilder builder = new OpenCorporaVocBuilder();
            builder.stemmer = new RussianStemmer();

            XmlReader reader = XmlReader.Create(file);

            reader.MoveToContent();
            int i = 0;

            List<string> grouplist = new List<string>();
            bool insideLemma = false;
            int nG = 1;
            while (reader.Read())
            {
                if (reader.Name.Equals("lemma") && (reader.NodeType.Equals(XmlNodeType.Element)))
                {
                    grouplist.Clear();
                    insideLemma = true;
                }
                else
                if (reader.Name.Equals("lemma") && (reader.NodeType.Equals(XmlNodeType.EndElement)))
                 {

                    builder.AddWordsGroup(grouplist.ToArray());
                   
                    insideLemma = false;
                    
                }
                else
                    if (insideLemma)
                {
                    string t = reader.GetAttribute("t");
                    if ((t!=null) && (t.Length != 0)) grouplist.Add(t);
                }
            }
            reader.Close();

            builder.build(outfile);

       
        }
    }

    class FreeLibVocBuilder: VocBuilder
    {
        public static void CreateFromFolder(string folder,string outfile)
        {
            FreeLibVocBuilder voc = new FreeLibVocBuilder();
            voc.stemmer = new EnglishStemmer();

            foreach (string file in Directory.GetFiles(folder, "*.*"))
            {
                StreamReader reader = File.OpenText(file);

                try
                {
                    do
                    {
                        string l = reader.ReadLine();
                        if (l != null)
                        {
                            string[] a = l.Split(' ');
                            if ((a.Length >= 2) && (a[0].Length > 0) && (a[1].Length > 0))
                            {
                                voc.AddWordsGroup(a.Take(2).ToArray());
                            }
                        }
                        else break;
                    }
                    while (true);
                }
                catch (EndOfStreamException e)
                {

                }
            }
            
            voc.build(outfile);
        }
    }
}
