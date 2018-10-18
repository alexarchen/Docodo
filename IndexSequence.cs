using System;
using System.Collections.Generic;

namespace Docodo
{
    class IndexSequence : List<ulong>
    {
        const ulong AND_INCREASE = 0x80000000;
        const ulong AND_MASK = (AND_INCREASE - 1);

        public IndexSequence() : base()
        {

        }

        public IndexSequence(IEnumerable<ulong> arr) : base(arr)
        {

        }

        public IndexSequence(IEnumerable<uint> arr) : base()
        {
            ulong _base = 0;
            foreach (uint el in arr)
            {
                if (el == 0xFFFFFFFF) _base += AND_INCREASE;
                else
                Add(_base + el);

            }
        }

        public bool order { get => R < 0; }
        public int R = 0; // distance between words
       // List<byte> Rank = new List<byte>();

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
            IndexSequence newSeq = new IndexSequence();
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
        // combine results by |
        public static IndexSequence operator |(IndexSequence seq1, IndexSequence seq2)
        {
            return (seq1);
        }
    }
}