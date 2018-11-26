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

          int N = 100;
          List<ulong> testList =  CreateList(N);

          IndexSequence test = new IndexSequence.Builder().AddRange(testList);

          Assert.True(test.SequenceEqual(testList));
        }

        [Fact]
        public void ShiftTest()
        {
          List<ulong> list = CreateList(100);
          list.Insert(0,0); // firt is 0
          IndexSequence seq = new IndexSequence.Builder().AddRange(list);
          IndexSequence seq2 = new IndexSequence (seq);
          // small shift
          ulong s1 = 100;
          seq2.Shift(s1);
          Assert.Equal(seq.Count,seq2.Count);
          Assert.Equal(seq.Count(),seq2.Count());
          var en = seq2.GetEnumerator();
          en.MoveNext();
          foreach (ulong s in seq){
            Assert.Equal(s1,en.Current-s);
            en.MoveNext();
          }
        
          // big shift
          seq2 = new IndexSequence (seq);
          s1 = 0xFFFFF;
          seq2.Shift(s1);
          Assert.Equal(seq.Count(),seq2.Count());
          en = seq2.GetEnumerator();
          en.MoveNext();
          foreach (ulong s in seq){
            Assert.Equal(s1,en.Current-s);
            en.MoveNext();
          }
        
          // huge shift
          seq2 = new IndexSequence (seq);
          s1 = 0xFFFFFFFF;
          seq2.Shift(s1);
          Assert.Equal(seq.Count(),seq2.Count());
          en = seq2.GetEnumerator();
          en.MoveNext();
          foreach (ulong s in seq){
            Assert.Equal(s1,en.Current-s);
            en.MoveNext();
          }



        }

        List<ulong> CreateList(int N){
          List<ulong> testList = new List<ulong>();
          ulong Last = 0;
          Random r = new Random();
          int c =0;
          for (int q=0;q<N;q++)
          {
            int delta = r.Next()&0x7FFFF;
            testList.Add((ulong)(Last+(ulong)delta));
            Last+=(ulong)delta;
          }
          return testList;
        }

        [Fact]
        public void SpeedTest(){

          int N = 1000000;
          List<ulong> testList =  CreateList(N);


          long start = System.Environment.TickCount;
           List<ulong> newList = new List<ulong>();
           foreach (ulong val in testList)
            newList.Add(val);
          long start2 = System.Environment.TickCount;
          IndexSequence test = new IndexSequence.Builder().AddRange(testList);
          long end = System.Environment.TickCount;
          Console.WriteLine($"Times: list: {start2-start}, indexsequence: {end-start2}");

          Assert.True((end-start2)/(start2-start)<4);

        }
    }
}