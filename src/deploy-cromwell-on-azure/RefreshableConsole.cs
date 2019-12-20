// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;

namespace CromwellOnAzureDeployer
{
    internal static class RefreshableConsole
    {
        private static readonly int initialCursorTop = Console.CursorTop;
        private static readonly List<Line> lines = new List<Line>();
        private static readonly object lockObj = new object();
        private static int cursorTopAdjustment = 0;

        public static Line WriteLine(string value = "", ConsoleColor? color = null)
        {
            return AddLine(value, color, true);
        }

        public static Line Write(string value = "", ConsoleColor? color = null)
        {
            return AddLine(value, color, false);
        }

        public static string ReadLine()
        {
            var result = Console.ReadLine();
            AdjustCursorTop(1);
            return result;
        }

        private static Line AddLine(string value, ConsoleColor? color, bool addNewLine)
        {
            lock (lockObj)
            {
                var line = new Line(addNewLine);
                lines.Add(line);
                line.Write(value, color);
                return line;
            }
        }

        private static void Render()
        {
            lock (lockObj)
            {
                var cursorTop = initialCursorTop - cursorTopAdjustment;
                
                if (cursorTop < 0)
                {
                    cursorTop = 0;
                }

                Console.CursorVisible = false;
                Console.CursorTop = cursorTop;
                Console.CursorLeft = 0;
                lines.ForEach(line => line.Render());
                Console.CursorVisible = true;
            }
        }

        private static void AdjustCursorTop(int numberOfNewLines)
        {
            if (Console.CursorTop == Console.BufferHeight - 1)
            {
                cursorTopAdjustment += numberOfNewLines;
            }
        }

        internal class Line
        {
            private readonly bool hasNewLine;
            private readonly List<Action> lineParts = new List<Action>();

            public Line(bool hasNewLine)
            {
                this.hasNewLine = hasNewLine;
            }

            public void Write(string value, ConsoleColor? color = null)
            {
                if (color.HasValue)
                {
                    lineParts.Add(() => Console.ForegroundColor = color.Value);
                }

                lineParts.Add(() => { AdjustCursorTop(value.Count(c => c == '\n')); Console.Write(value); });

                RefreshableConsole.Render();
            }

            public void Render()
            {
                lineParts.ForEach(part => part());

                if (hasNewLine)
                {
                    AdjustCursorTop(1);
                    Console.WriteLine();
                }

                Console.ResetColor();
            }
        }
    }
}
