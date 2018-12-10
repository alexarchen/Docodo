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
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using Docodo;
using System.Globalization;

namespace Docodo
{
    class Program
    {

        static private CancellationTokenSource cancelationToken;
    
        
        static public Index ind; // Global index
        
        static void CreateVoc(string name){
             if (name.ToLower().Equals("ru"))
                {
                    Console.WriteLine("Creating russian voc (wait a minute)...");
                    OpenCorporaVocBuilder.CreateFromOpenCorpora("Dict\\ru\\dict.opcorpora.xml", "Dict\\ru.voc");
                }
                else
                {
                    Console.WriteLine($"Creating {name} voc (wait a minute)...");
                    FreeLibVocBuilder.CreateFromFolder($"Dict\\{name}", $"Dict\\{name}.voc");
              }
        }

        static void Main(string[] args)
        {
            Console.Write("DOCODO Search Engine\nCopyrigt (c) 2018 Alexey Zakharchenko \n");
            int nPort = 9001;
            try
            {
                nPort = Int32.Parse((from a in args where a.StartsWith("-p:") select a).Last().Substring(3));
            }
            catch (Exception e) { }

            if (args.Contains("server"))
             new DocodoServer(nPort);

            List<Vocab> vocs = new List<Vocab>();
            Console.Write("Loaded vocs: ");
            foreach (string file in Directory.GetFiles("Dict\\", "*.voc"))
            {
                vocs.Add(new Vocab(file));
                Console.Write(file.Substring(file.LastIndexOf("\\") + 1).Split('.')[0]+" ");
            }
            if (vocs.Count == 0) Console.Write("No!");
            Console.Write("\n");
            
            // TODO: create voc command, like -cv:en
            foreach (string crvoc in (from a in args where a.StartsWith("-cv:") select a.Substring(4)))
            {
                CreateVoc(crvoc);
            }

            String basepath = ".";
            try
            {
                basepath = (from a in args where a.StartsWith("-i:") select a).Last().Substring(3);
            }
            catch (Exception e) { }
            ind = new Index(basepath, false, vocs.ToArray<Vocab>());
            

            foreach (string source in (from a in args where a.StartsWith("-source:") select a.Substring(8)))
            {
                var spl = source.Split(',');
                if (spl[0].Equals("doc"))
                    ind.AddDataSource(new DocumentsDataSource("doc", spl[1]));
                else
                 if (spl[0].Equals("web"))
                {
                    WebDataSource websource = new WebDataSource("web", spl[1],spl.Length>2?spl[2]:"");
//                    websource.MaxItems = 100;
                    ind.AddDataSource(websource);
                }
                else
                 if (spl[0].Equals("xml"))
                {
                    XmlDataSource websource = new XmlDataSource("xml", spl[1]);
                    ind.AddDataSource(websource);
                }
                else
                 if (spl[0].Equals("mysql"))
                {
                    string Connect = null;
                    string Query = null;
                    string FieldName = null;
                    string BasePath = null;
                    try
                    {
                        foreach (string line in File.ReadAllLines(spl[1]))
                        {
                            string[] name = line.Split("=");
                            if (name[0].Equals("Connect")) { Connect = line.Substring(8); }
                            if (name[0].Equals("Query")) { Query = line.Substring(6); }
                            if (name[0].Equals("BasePath")) { BasePath = line.Substring(9); }
                            if (name[0].Equals("IndexType")) { FieldName = name[1]; }
                        }

                        if (Connect == null) throw new InvalidDataException("No Connect key");
                        if (Query == null) throw new InvalidDataException("No Query key");
                        if (FieldName == null) throw new InvalidDataException("No IndexType key");
                        if (BasePath == null) throw new InvalidDataException("No BasePath key");
                        ind.AddDataSource(new MySqlDBDocSource("mysql_" + spl[1], BasePath, Connect, Query, FieldName));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error adding mysql source: " + e.Message);
                    }
                }
            }

            ind.LoadStopWords("Dict\\stop.txt");
            foreach (string sf in (from a in args
                                   where a.StartsWith("-stops:")
                                   select a.Substring(7)))
                 ind.LoadStopWords(sf);
            
            

            cancelationToken = ind.cancelationToken; // token to cancel something

            if (ind.CanSearch) Console.WriteLine("Index loaded, contains {0} words", ind.Count);
            
            ConsoleKey c;
            do
            {

                Console.WriteLine("Press "+(ind.CanIndex?"I to index, ":"") + (ind.CanSearch ? " S to search, O for info, " : "") + "V to manage vocs, E to exit...");
                c = Console.ReadKey(false).Key;

                if (c==ConsoleKey.V)
                {
                    while (true)
                    {
                     Console.WriteLine("-----------\nCreate vocabs\nType voc name from list below or e to exit:");
                     foreach (string f in Directory.GetDirectories("Dict\\").Select((s)=>s.Substring(s.LastIndexOf('\\')+1)))
                     {
                         Console.Write(f+",");
                     }
                     Console.WriteLine("");

                     string line = Console.ReadLine();
                     if (!line.Equals("e"))
                     {
                         CreateVoc(line);
                     }
                     else break;
                    }

                }
                if (c==ConsoleKey.O)
                {
                  ShowInfo();
                }
                else
                if (c==ConsoleKey.S)
                {
                    Console.WriteLine("Type text to search, e - exit");
                    Console.Write("req:");
                    Console.InputEncoding = Encoding.Unicode;// Windows1251.GetEncoding();
                    string req;
                    while (!(req = ReadSearchRequest()).Equals("e"))
                    {
                         
                        Index.SearchResult result = ind.Search(req);

                        Console.WriteLine("Found {0} pages in {1} docs:", result.foundPages.Count, result.foundDocs.Count);
                        foreach (var d in result.foundDocs)
                        {
                            Console.WriteLine($"Doc: {d.Name}, Found {d.pages.Count} pages");
                            foreach (var p in d.pages) {
                                Console.WriteLine($"  Page {p.id} ({p.pos.Count} times)");
                                Console.WriteLine("    Text: "+ p.text);
                            }
                        }
                        Console.Write("req:");
                    }




                }
                else
                if (c == ConsoleKey.I)
                {
                    Console.WriteLine($"Start Indexing ...");

                    // user input task
                    
                   
                    Task cT = new Task(() =>
                    {
                        do
                        {
                            while (Console.In.Peek() == -1)
                            {
                                Thread.Sleep(200);
                                if (cancelationToken == null) break;
                            }

                            if (cancelationToken != null)
                            {

                                //char bc = (char); //In.Read();

                                if (Console.ReadKey().Key == ConsoleKey.C)
                                {
                                    Console.WriteLine("Indexing was interrupted by user.");
                                    cancelationToken.Cancel();
                                    break;
                                }
                            }
                        }
                        while (cancelationToken!=null);

                    });
                    //cT.Start(); // listen console to interrupt 
                    try
                    {
                        ind.CreateAsync().Wait();
                    }
                    catch (OperationCanceledException e)
                    {

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error creaing index");
                    }
                    cancelationToken = null;
                   

                    Console.WriteLine("Indexing complited.");

                }
            }
            while (c != ConsoleKey.E);
        }


static string ReadSearchRequest(){
    bool isInReq = true;
    char c;
    string res = "";
    int Left = Console.CursorLeft;
    do{
      c = Console.ReadKey().KeyChar;

      if (c!='\r')
      {
       if (c=='\b')  {if (res.Length>0) res = res.Remove(res.Length-1);}
       else
        res+=c; 

       ind.GetSuggessions(res,12).ContinueWith((s)=>{
           if (isInReq){
           int lp =Console.CursorLeft;
           int tp = Console.CursorTop;
           int count=0;
           ConsoleColor color = Console.ForegroundColor;
           Console.ForegroundColor = ConsoleColor.Gray;
           for (int q=0;q<12;q++) {Console.SetCursorPosition(0,tp+1+q); Console.Write(new String(' ',Console.WindowWidth));}
           foreach (var ss in s.Result)
           {
            Console.SetCursorPosition(Left,tp+count+1);
            Console.Write(res+ss);
            count++;
           }

           Console.SetCursorPosition(lp,tp);
           Console.ForegroundColor = color;
           }
       });

      }
    }while (c!='\r');

 isInReq = false;
 return res;
}
      static void ShowInfo(int numb=20)
      {
                        Console.WriteLine($"Index contains: {ind.Count} words");
                        var hist = Index.CalcHistogram(ind);
                        Console.WriteLine("Histogram:");
                        foreach (var item in hist.Take(numb))
                        {
                            Console.WriteLine($"{item.Key}: "+String.Format("{0:f2}%", 100.0*item.Value/ind.MaxCoord));
                        }
                        

      }
    }

    
    

}
