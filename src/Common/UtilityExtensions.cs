// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace CommonUtilities
{
    public static class UtilityExtensions
    {
        #region AddRange
        public static void AddRange<T>(this IList<T> list, IEnumerable<T> values)
        {
            foreach (var value in values)
            {
                list.Add(value);
            };
        }

        //public static void AddRange<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, IDictionary<TKey, TValue> values)
        //{
        //    foreach (var value in values)
        //    {
        //        dictionary.Add(value);
        //    };
        //}
        #endregion
    }
}
