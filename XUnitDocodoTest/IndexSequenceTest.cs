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

          List<ulong> testList = new List<ulong>();
          int N = 100;
          ulong Last = 0;
          Random r = new Random();
          int c =0;
          for (int q=0;q<N;q++)
          {
            int delta = r.Next()&0xFFFFF;
            testList.Add((ulong)(Last+(ulong)delta));
            Last+=(ulong)delta;
            c++;
            if (c<10) Console.Write(" "+Last);
          }

          Console.WriteLine(" ");

          IndexSequence test = new IndexSequence.Builder().AddRange(testList);
          foreach(ulong l in test.Take(10))
            Console.Write(" "+l);

          Console.WriteLine(" ");

          Assert.True(test.SequenceEqual(testList));
        }
    }
}