// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CromwellOnAzureDeployer
{
    internal static class ConsoleEx
    {
        private static object lockObj = default;
        private static Size offset = default;
        private static bool isRedirected = false;
        private static Size? sizeIfNotResized;
        private static Writer writer = default;

        // This method is reentrant and can safely be called multiple times
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

                sizeIfNotResized = new Size(Console.BufferWidth, Console.BufferHeight);

                writer = new Writer(Console.Out);
                Console.SetOut(writer);

                // Possible extension point: use a version of Writer that colorizes StdError
                if (!Console.IsErrorRedirected)
                {
                    // Since the output is going to the same place ($CONOUT or its equivalent) we can just use the same writer
                    Console.SetError(writer);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Line WriteLine(string value = null, ConsoleColor? color = null)
            => new(value, color, true);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Line Write(string value = null, ConsoleColor? color = null)
            => new(value, color, false);

        public static string ReadLine()
        {
            var result = Console.ReadLine();
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                Init();
                lock (lockObj)
                {
                    if (Console.CursorTop == Console.BufferHeight - 1)
                    {
                        offset += new Size(0, 1);
                    }
                }
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool WasResized() // Assumed to only be called with the lock held
        {
            if (sizeIfNotResized is null) { return true; }

            if (Console.BufferHeight != sizeIfNotResized.Value.Height || Console.BufferWidth != sizeIfNotResized.Value.Width)
            {
                sizeIfNotResized = null;
                return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Point GetOffsetCursorPosition()
            => GetPoint(Console.GetCursorPosition()) + offset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Point GetPoint((int Left, int Top) position)
            => new(position.Left, position.Top);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldProcess(string text)
        {
            Init();
            if (isRedirected)
            {
                lock (lockObj)
                {
                    Console.Out.WriteLine(text);
                }
                return false;
            }
            else
            {
                return true;
            }
        }

        // Returns the expected end point of the rendered text if the rendering starts @ startPoint, given the terminals current WindowWidth
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Point GetEndPoint(string text, Point startPoint)
        {
            var shape = GetShape(text, startPoint.X);
            return new(shape.Width, shape.Height - 1 + startPoint.Y);
        }

        // Returns the expected shape of the rendered text relative to the starting row (Width == left-most column, not width)
        private static Size GetShape(string text, int firstLineColumn)
        {
            var windowWidth = Console.WindowWidth;
            var startCol = firstLineColumn;

            var shape = (text?.Split(Console.Out.NewLine).SelectMany(WrapString) ?? Enumerable.Empty<string>()).ToList();
            return new((shape.Count > 1 ? 0 : firstLineColumn) + shape.LastOrDefault()?.Length ?? 0, shape.Count);

            IEnumerable<string> WrapString(string value)
            {
                while (!string.IsNullOrEmpty(value))
                {
                    yield return SplitString(ref value);
                }
            }

            string SplitString(ref string value)
            {
                var width = windowWidth - startCol;
                startCol = 0;

                var s = new string(value.TakeWhile(TakeFunc).ToArray());
                value = value[s.Length..];
                return s;

                bool TakeFunc(char c)
                {
                    width -= char.GetUnicodeCategory(c) switch
                    {
                        UnicodeCategory.Surrogate => char.IsLowSurrogate(c),
                        UnicodeCategory.NonSpacingMark => false,
                        UnicodeCategory.EnclosingMark => false,
                        _ => true,
                    } ? 1 : 0;

                    return -1 != Math.Sign(width);
                }
            }
        }

        internal sealed class Line
        {
            private string contents = string.Empty;

            internal ConsoleColor? ForegroundColor { get; private set; }
            internal Point? AppendPoint { get; private set; } = null;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Line(string value, ConsoleColor? color, bool terminateLine)
            {
                value = value?.Normalize();
                if (!ShouldProcess(value))
                {
                    this.contents += value;
                    return;
                }

                WriteImpl(value, color, terminateLine);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Write(string value = null, ConsoleColor? color = null)
                => WriteImpl(value, color, false);

            private void WriteImpl(string value, ConsoleColor? color, bool terminateLine)
            {
                value = value?.Normalize();
                this.contents += value ?? string.Empty;
                if (!ShouldProcess(this.contents))
                {
                    return;
                }

                lock (lockObj)
                {
                    this.ForegroundColor = color ?? ForegroundColor;

                    var wasResized = WasResized();
                    if (wasResized)
                    {
                        value = contents;
                        terminateLine = true;
                    }

                    var (saveLeft, saveTop) = Console.GetCursorPosition();
                    var restoreCursor = !wasResized && AppendPoint.HasValue;

                    try
                    {
                        AppendPoint ??= GetOffsetCursorPosition();

                        var startPos = wasResized ? GetOffsetCursorPosition() : AppendPoint.Value;
                        var curPos = startPos - offset;

                        if (curPos.Y < 0) // This line has now scrolled out of reach
                        {
                            return; // TODO: print new line as if redirection were in place?
                        }

                        // calculate position of new AppendPoint
                        var endPos = GetEndPoint(value, startPos);

                        // Send output
                        Console.SetCursorPosition(curPos.X, curPos.Y);
                        if (startPos.Y != endPos.Y && !terminateLine)
                        {
                            // TODO: insert (endPos.Y - startPos.Y) lines after current line (startPos.Y - offest.Height). This is done differently depending on the platform.
                        }
                        if (ForegroundColor is not null)
                        {
                            Console.ForegroundColor = ForegroundColor.Value;
                        }
                        writer.SystemWriter.Write(value);

                        // Adjust state
                        if (Console.BufferHeight <= (endPos.Y - offset.Height))
                        {
                            offset += new Size(0, endPos.Y - offset.Height - Console.BufferHeight + 1);
                        }

                        AppendPoint = GetOffsetCursorPosition();
                        var finalPos = AppendPoint.Value;
                        if (terminateLine)
                        {
                            //offset += new Size(0, 1);
                            writer.SystemWriter.WriteLine();
                            finalPos = GetOffsetCursorPosition();
                        }

                        if (endPos.X == Console.BufferWidth && !terminateLine) // Cursor is left at the last glyph written instead of right after the last glyph written when that last glyph is written into the right-most column.
                        {
                            endPos = new Point(0, endPos.Y + 1);
                            AppendPoint = new Point(0, AppendPoint.Value.Y + 1);
                            finalPos = new Point(0, finalPos.Y + 1);
                        }

                        offset += (terminateLine ? new Size(0, endPos.Y + 1) : new Size(endPos)) - new Size(terminateLine ? 0 : finalPos.X, finalPos.Y);
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

        // Intercepts Console.Write~() calls to improve synchronization between actual terminal state and our representation of that state
        // This isn't perfect, there are probably edge cases that will break this
        private sealed class Writer : TextWriter
        {
            internal TextWriter SystemWriter { get; }

            public override Encoding Encoding => SystemWriter.Encoding;

            public override string NewLine { get => SystemWriter.NewLine; set => SystemWriter.NewLine = value; }

            public override IFormatProvider FormatProvider => SystemWriter.FormatProvider;

            public Writer(TextWriter writer)
                => SystemWriter = writer;

            public override void Write(char value)
            {
                Init();
                lock (lockObj)
                {
                    SystemWriter.Write(value);
                    if (value == '\n' && Console.CursorTop == Console.BufferHeight - 1)
                    {
                        offset += new Size(0, 1);
                    }
                }
            }

            public override void Flush()
                => SystemWriter.Flush();

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    SystemWriter.Dispose();
                }

                base.Dispose(disposing);
            }

            public override ValueTask DisposeAsync()
                => new(Task.WhenAll(new Task[]
                {
                    SystemWriter.DisposeAsync().AsTask(),
                    base.DisposeAsync().AsTask()
                }));
        }
    }
}
