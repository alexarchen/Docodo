using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;

namespace Docodo
{
    //sequence of ascending ulong values that internaly stores encoded ushorts
    // for internal use with Index only 
    public class IndexSequence : IEnumerable<ulong>
    {
        
        const short BITS = 15;
        const ushort OVERFLOW = 1<<BITS;
        const ushort MASK = OVERFLOW-1;
        public IndexSequence() : base()
        {

        }

        public IndexSequence(IndexSequence seq) 
        {
            self = new List<ushort>(seq.self);

        }

        public IndexSequence(IEnumerable<ushort> arr) 
        {
           self = new List<ushort>(arr);
        }


        public IEnumerator<ulong> GetEnumerator()
        {

            return new SeqEnumerator(this);

        }

         IEnumerator  IEnumerable.GetEnumerator(){
            return GetEnumerator();
        }
        // only for this[] operator, returns number of internal ushort sequence
        // use LINQ Count() instead to calculate true lengths of sequence
        public int Count=>self.Count;
        public class Builder {
            IndexSequence seq = new IndexSequence();
            private ulong Last = 0;
            private int min = 0;

            // min is minimum diffenrece between added elements,
            // if distance is less then no addition performed
            public Builder(int min=0){
                this.min = min;
            }

            public int R {get => seq.R; set => seq.R = value;}

            public Builder SetR(int r){
                R = r;
                return this;
            }
            public Builder Add(ulong l){
                if (l<Last) throw new InvalidDataException("Must add ascending values");
                if ((l-Last<=(ulong)min) && (seq.Count>0)) return this; // check for min distance
                
                ulong diff =l-Last;
                do{
                    seq.self.Add((ushort)((diff>MASK?OVERFLOW:0)|((ushort)((diff&MASK)))));
                    diff>>=BITS;
                }
                while (diff>0);
                Last = l;
            return (this);
            }

            public Builder AddRange(IEnumerable<ulong> range){
                foreach (ulong v in range)
                 Add (v);
             return (this);
            }


            public IndexSequence build()
            {
             return seq;
            }

            public static implicit operator IndexSequence (IndexSequence.Builder bild){
                return bild.build();
            }

        }

        public class SeqEnumerator : IEnumerator<ulong>
        {
            private IndexSequence _seq;
            ulong Last =0;
            IEnumerator<ushort> enumerator;
            public SeqEnumerator(IndexSequence seq){
                _seq = seq;
                Reset();
            }

            public bool MoveNext(){
                if (enumerator!=null){

                    bool bNeedMore = false;
                    int shift = 0;

                    do{

                    
                    if (enumerator.MoveNext()){
                         ushort value = enumerator.Current;
                         bNeedMore = (value&OVERFLOW)!=0;
                         Last+=(((ulong)(value&MASK))<<shift);
                         shift+=BITS;
                     }
                     else return false;

                    }while (bNeedMore);

                    return true;
                }
                return false;
            }

            public object Current {get => Current;}
            ulong IEnumerator<ulong>.Current {
                get{

                return Last;

            }
            }

            public  void Reset(){
              Last=0;
              if (enumerator!=null) enumerator.Dispose();
              enumerator = _seq.self.GetEnumerator();
            }

            public void Dispose(){
              if (enumerator!=null) enumerator.Dispose();
              enumerator = null;
            }

        }

        List<ushort> self = new List<ushort>(); 
        // index i < Count
        public ushort this[int i]{ get => self[i];}

        public bool order { get => R < 0; }
        public int R = 0; // distance between words
       // List<byte> Rank = new List<byte>();


        public void Write(BinaryWriter bin)
        {
          bin.Write(self.Count);
          
          foreach (ushort s in self)
           bin.Write(s);
        }

        public void Read(BinaryReader read){
            int n = read.ReadInt32();
            ushort [] arr = new ushort[n];
            byte[] bytes = read.ReadBytes(sizeof(ushort) * n);
            Buffer.BlockCopy(bytes, 0, arr, 0, sizeof(ushort) * n);
            self = new List<ushort>(arr);
        }

        // shift all values 
        public void Shift(ulong shift){
            if ((Count==0) || (shift==0)) return;
            var en = this.GetEnumerator();
            en.MoveNext(); 
            IndexSequence s = new Builder().Add(shift+en.Current);
            int q=0;
            do{}while ((self[q++]&OVERFLOW)!=0);
            // replacing first value
            self.RemoveRange(0,q);
            self.InsertRange(0,s.self);
           
        }

        // Combine two parts of words, order does matter
        public static IndexSequence operator *(IndexSequence seq1, IndexSequence seq2)
        {
//            int R = seq1.R;
//            seq1.R = -1;
            IndexSequence news = seq1 & seq2;

            // remove repeating coords
            //IndexSequence news1 = new IndexSequence();
     //       seq1.R = R;
            return (news);
        }
        // Combine results by & using distanse and order

        public static IndexSequence operator &(IndexSequence seq1, IndexSequence seq2)
        {
            
            uint absR = (uint)Math.Abs(seq1.R);
            int R = seq1.R;
            IndexSequence.Builder newSeq = new IndexSequence.Builder();
            //List<byte> newRank = newSeq.Rank;
            newSeq.R = seq1.R;
            List<ulong> newGroup = new List<ulong>();


            IEnumerator<ulong>[] enums = { seq1.GetEnumerator(), seq2.GetEnumerator() };
            bool[] move = { true, true };
            bool[] can = { true, true };
            bool[] IsInGr = { false, false };
            do
            {
                ulong valToAdd = 0;
                if (!move[0] && (!move[1])) break;
                if ((move[0]) && (can[0])) { can[0] = enums[0].MoveNext(); }
                if ((move[1]) && (can[1])) { can[1] = enums[1].MoveNext(); }
                move[0] = move[1] = false;
                if (!can[0] && !can[1]) break;

                if (!can[0]) { valToAdd = enums[1].Current; move[1] = true; }
                else
                if (!can[1]) { valToAdd = enums[0].Current; move[0] = true; }
                else
                if (enums[0].Current < enums[1].Current) { valToAdd  = enums[0].Current; move[0] = true; }
                else
                if (enums[0].Current > enums[1].Current) { valToAdd = enums[1].Current; move[1] = true; }
                else { move[0] = move[1] = true;valToAdd = enums[1].Current; }


                if (newGroup.Count > 0)
                {// check
                    bool bfinish = false;
                    if ((absR != 0) && (valToAdd - newGroup[newGroup.Count - 1] > absR)) bfinish = true;
                    else
                    if (!((R >= 0) || (move[0]) || (IsInGr[0])))
                     bfinish = true; 

                    if (bfinish)
                    {
                        if (IsInGr[0] && IsInGr[1])
                            newSeq.AddRange(newGroup);
                        IsInGr[0] = IsInGr[1] = false; newGroup.Clear();
                        if (!can[0] || !can[1]) break;

                    }
                }
                
                // add to group
                 if (move[0]) IsInGr[0] = true;
                 if (move[1]) IsInGr[1] = true;
                 newGroup.Add(valToAdd);
                
            }
            while (can[0] || can[1]);

            if (IsInGr[0] && IsInGr[1])
                newSeq.AddRange(newGroup);

            return (newSeq);
        }
        // combine results by logical OR
        public static IndexSequence operator +(IndexSequence seq1, IndexSequence seq2)
        {
            IndexSequence.Builder res = new IndexSequence.Builder();
            //res.Capacity = Math.Max(seq1.Count, seq2.Count);
            IEnumerator<ulong> [] seqe = { seq1.GetEnumerator(), seq2.GetEnumerator() };
            bool[] Move = { true, true };
            bool[] Exists = { false, false };
            do
            {
                for (int q = 0; q < 2; q++)
                { if (Move[q]) Exists[q] = seqe[q].MoveNext();
                    Move[q] = false;
                }

                if (Exists[0] || Exists[1])
                {
                    if (!Exists[1]) { res.Add(seqe[0].Current); Move[0] = true; }
                    else if (!Exists[0]) { res.Add(seqe[1].Current); Move[1] = true; }
                    else
                    { // both

                        if (seqe[0].Current <= seqe[1].Current) Move[0] = true;
                        if (seqe[1].Current <= seqe[0].Current) Move[1] = true;
                        if (Move[0]) res.Add(seqe[0].Current);
                        else res.Add(seqe[1].Current);
                    }

                }
            }
            while (Exists[0] || Exists[1]); 

            return (res);
        }
    }
}