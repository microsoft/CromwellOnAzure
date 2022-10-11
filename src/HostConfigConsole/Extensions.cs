// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

namespace HostConfigConsole
{
    internal static class Extensions
    {
        public static void ForEach<T>(this IEnumerable<T> values, Action<T> action)
        {
            ArgumentNullException.ThrowIfNull(values);
            ArgumentNullException.ThrowIfNull(action);

            foreach (var value in values)
            {
                action(value);
            }
        }
    }
}
