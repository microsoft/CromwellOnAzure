// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace HostConfigCompiler
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public static class Extensions
    {
        // https://www.appsloveworld.com/linq/100/10/how-to-use-zip-on-three-ienumerables-source-code
        public static IEnumerable<TResult> Zip<TFirst, TSecond, TThird, TResult>
            (this IEnumerable<TFirst> source, IEnumerable<TSecond> second, IEnumerable<TThird> third, Func<TFirst, TSecond, TThird, TResult> selector)
        {
            using var iterator1 = source.GetEnumerator();
            using var iterator2 = second.GetEnumerator();
            using var iterator3 = third.GetEnumerator();

            while (iterator1.MoveNext() && iterator2.MoveNext() && iterator3.MoveNext())
            {
                yield return selector(iterator1.Current, iterator2.Current, iterator3.Current);
            }
        }

        // A version of this was added in .Net6. The method above gets us almost there. This completes the circle.
        public static IEnumerable<(TFirst first, TSecond second, TThird third)> Zip<TFirst, TSecond, TThird>
            (this IEnumerable<TFirst> source, IEnumerable<TSecond> secondList, IEnumerable<TThird> thirdList)
            => Zip(source, secondList, thirdList, (source, second, third) => (source, second, third));
    }
}
