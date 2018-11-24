using Docodo;
using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace XUnitDocodoTest
{

      public class IndexSequenceTest 
    {

        [Fact]
        public void ConvertTest(){

          Console.WriteLine("Test Longs: ");

          List<ulong> testList = new List<ulong>();
          int N = 100;
          ulong Last = 0;
          Random r = new Random();
          int c =0;
          for (int q=0;q<N;q++)
          {
            int delta = r.Next()&0x7FFFF;
            testList.Add((ulong)(Last+(ulong)delta));
            Last+=(ulong)delta;
            if (c++<10) Console.Write(String.Format("{0:X} ",Last));
          }

          IndexSequence test = new IndexSequence.Builder().AddRange(testList);

          Console.WriteLine($"\nUshorts ({test.Count}): ");
          
          for (int q=0;q<Math.Min(10,test.Count);q++)
           Console.Write(String.Format("{0:X} ",test[q]));

          Console.WriteLine("\nResult ulongs: ");

          foreach(ulong l in test.Take(10))
            Console.Write(String.Format("{0:X} ",l));

          Console.WriteLine(" ");

          Assert.True(test.SequenceEqual(testList));
        }

        [Fact]
        public void SpeedTest(){

          List<ulong> testList = new List<ulong>();
          int N = 1000000;
          ulong Last = 0;
          Random r = new Random();
          int c =0;
          for (int q=0;q<N;q++)
          {
            int delta = r.Next()&0x7FFFF;
            testList.Add((ulong)(Last+(ulong)delta));
            Last+=(ulong)delta;
          }

          long start = System.Environment.TickCount;
           List<ulong> newList = new List<ulong>();
           foreach (ulong val in testList)
            newList.Add(val);
          long start2 = System.Environment.TickCount;
          IndexSequence test = new IndexSequence.Builder().AddRange(testList);
          long end = System.Environment.TickCount;
          Console.WriteLine($"Times: list: {start2-start}, indexsequence: {end-start2}");

          Assert.True((end-start2)/(start2-start)<2);

        }
    }
}