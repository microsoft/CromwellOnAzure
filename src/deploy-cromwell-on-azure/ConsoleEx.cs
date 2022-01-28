// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using static PInvoke.Kernel32;

namespace CromwellOnAzureDeployer
{
    internal static class ConsoleEx
    {
        private static object lockObj = default;
        private static Size offset = default;
        private static Point lastCursor = default;
        private static bool isRedirected = false;

        public static void Init()
        {
            if (Interlocked.CompareExchange(ref lockObj, new(), null) is not null)
            {
                return;
            }

            lock (lockObj)
            {
                if (Console.IsOutputRedirected)
                {
                    isRedirected = true;
                    return;
                }

                if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                {
                    if (!GetConsoleMode(GetStdHandle(StdHandle.STD_OUTPUT_HANDLE), out var bufferMode))
                    {
                        Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }
                    if (!SetConsoleMode(GetStdHandle(StdHandle.STD_OUTPUT_HANDLE), bufferMode | ConsoleBufferModes.ENABLE_VIRTUAL_TERMINAL_PROCESSING))
                    {
                        Marshal.ThrowExceptionForHR(Marshal.GetHRForLastWin32Error());
                    }
                }

                lastCursor = GetOffsetCursorPosition();
            }
        }

        public static Line WriteLine(string value = null, ConsoleColor? color = null)
            => new(value, color, true);

        public static Line Write(string value = null, ConsoleColor? color = null)
            => new(value, color, false);

        public static string ReadLine()
        {
            Init();
            var result = Console.ReadLine();
            lock (lockObj)
            {
                if (Console.CursorTop == Console.BufferHeight - 1)
                {
                    offset += new Size(0, 1);
                }
            }
            return result;
        }

        private static Point GetOffsetCursorPosition()
        {
            var (left, top) = Console.GetCursorPosition();
            return new Point(left, top) + offset;
        }

        private static bool ShouldProcess(string value, bool terminateLine)
        {
            Init();

            if (isRedirected)
            {
                lock (lockObj)
                {
                    if (terminateLine)
                    {
                        Console.WriteLine(value);
                    }
                    else
                    {
                        Console.Write(value);
                    }
                }
                return false;
            }
            else
            {
                return true;
            }
        }

        internal sealed class Line
        {
            private string contents = string.Empty;
            private bool terminateLine;

            internal ConsoleColor? ForegroundColor { get; private set; }
            internal Point? AppendPoint { get; private set; } = null;

            public Line(string value, ConsoleColor? color, bool terminateLine)
            {
                value = value?.Normalize();
                if (!ShouldProcess(value, terminateLine))
                {
                    this.contents += value;
                    return;
                }

                this.terminateLine = terminateLine;
                Write(value, color);
            }

            public void Write(string value = null, ConsoleColor? color = null)
            {
                value = value?.Normalize();
                this.contents += value ?? string.Empty;
                if (!ShouldProcess(this.contents, false))
                {
                    return;
                }

                lock (lockObj)
                {
                    this.ForegroundColor = color;

                    var (saveLeft, saveTop) = Console.GetCursorPosition();
                    var restoreCursor = AppendPoint.HasValue;

                    try
                    {
                        AppendPoint ??= GetOffsetCursorPosition();
                        var pos = AppendPoint.Value;
                        var curPos = pos - offset;
                        if (curPos.Y < 0) // This line has now scrolled out of reach
                        {
                            return; // TODO: print new line as if redirection were in place?
                        }

                        if (pos.Y > lastCursor.Y)
                        {
                            offset += new Size(pos - new Size(lastCursor));
                        }

                        Console.SetCursorPosition(curPos.X, curPos.Y);

                        var wrapWidth = Console.WindowWidth;
                        var wrapCol = pos.X;

                        // calculate position of new AppendPoint
                        var shape = (value?.Split(Console.Out.NewLine).SelectMany(WrapString) ?? Enumerable.Empty<string>()).ToList();
                        pos = new Point((shape.Count > 1 ? 0 : pos.X) + shape.LastOrDefault()?.Length ?? 0, shape.Count - 1 + pos.Y);

                        // Send output
                        if (color is not null)
                        {
                            Console.ForegroundColor = color.Value;
                        }

                        Console.Write(value);

                        // Determine if screen/buffer is scrolling
                        AppendPoint = GetOffsetCursorPosition();
                        Point finalPos = AppendPoint.Value;
                        if (terminateLine)
                        {
                            Console.WriteLine();
                            finalPos = GetOffsetCursorPosition();
                        }

                        var delta = (terminateLine ? new Size(0, pos.Y + 1) : new Size(pos)) - new Size(terminateLine ? 0 : finalPos.X, finalPos.Y);
                        terminateLine = false;
                        offset += delta;
                        if (!restoreCursor)
                        {
                            lastCursor = finalPos + delta;
                        }

                        IEnumerable<string> WrapString(string value)
                        {
                            for (var s = GetSubstring(ref value); !string.IsNullOrEmpty(s); s = GetSubstring(ref value))
                            {
                                yield return s;
                            }
                        }

                        string GetSubstring(ref string value)
                        {
                            var size = wrapWidth - wrapCol;
                            wrapCol = 0;

                            var surrogates = false;
                            var s = new string(value.TakeWhile(TakeFunc).ToArray());
                            value = value[s.Length..];
                            return s;

                            bool TakeFunc(char c, int i)
                            {
                                size -= char.GetUnicodeCategory(c) switch
                                {
                                    UnicodeCategory.Surrogate => SurrogateCount(),
                                    UnicodeCategory.NonSpacingMark => 0,
                                    UnicodeCategory.EnclosingMark => 0,
                                    _ => 1,
                                };

                                return 0 != size;
                            }

                            int SurrogateCount()
                            {
                                surrogates = !surrogates;
                                return surrogates ? 0 : 1;
                            }
                        }
                    }
                    finally
                    {
                        if (restoreCursor)
                        {
                            Console.SetCursorPosition(saveLeft, saveTop);
                        }
                        Console.ResetColor();
                    }
                }
            }
        }
    }
}
