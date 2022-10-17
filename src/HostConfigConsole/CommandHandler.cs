// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.Invocation;

namespace HostConfigConsole
{
    internal abstract class CommandHandler : ICommandHandler
    {
        protected Symbol[]? symbols;

        protected CommandHandler(Symbol[]? symbols)
            => this.symbols = symbols;

        protected IEnumerable<Symbol> GetSymbols()
            => symbols ?? Enumerable.Empty<Symbol>();

        protected T? GetValue<T>(InvocationContext context, Symbol symbol)
            => symbol switch
            {
                Argument a => GetValueForArgument<T>(context, a),
                Option o => GetValueForOption<T>(context, o),
                _ => throw new NotSupportedException(),
            };

        protected T GetValueForArgument<T>(InvocationContext context, Argument symbol)
            => context.ParseResult.GetValueForArgument((Argument<T>)symbol);

        protected T? GetValueForOption<T>(InvocationContext context, Option symbol)
            => context.ParseResult.GetValueForOption((Option<T>)symbol);

        public virtual int Invoke(InvocationContext context)
            => InvokeAsync(context).GetAwaiter().GetResult();

        public virtual Task<int> InvokeAsync(InvocationContext context)
            => Task.FromResult(Invoke(context));
    }

    internal abstract class CommandHandler<T1, T2, T3> : CommandHandler
    {
        protected CommandHandler(Symbol[]? symbols)
            : base(symbols) { }

        protected abstract Task InvokeAsync(InvocationContext context, T1? t1, T2? t2, T3? t3);

        public sealed override async Task<int> InvokeAsync(InvocationContext context)
        {
            var symbols = GetSymbols().ToArray();
            if (symbols.Length != 3) throw new NotSupportedException();
            await InvokeAsync(context, GetValue<T1>(context, symbols[0]), GetValue<T2>(context, symbols[1]), GetValue<T3>(context, symbols[2]));
            return context.ExitCode;
        }
    }

    internal abstract class CommandHandler<T1, T2, T3, T4> : CommandHandler
    {
        protected CommandHandler(Symbol[]? symbols)
            : base(symbols) { }

        protected abstract Task InvokeAsync(InvocationContext context, T1? t1, T2? t2, T3? t3, T4? t4);

        public sealed override async Task<int> InvokeAsync(InvocationContext context)
        {
            var symbols = GetSymbols().ToArray();
            if (symbols.Length != 4) throw new NotSupportedException();
            await InvokeAsync(context, GetValue<T1>(context, symbols[0]), GetValue<T2>(context, symbols[1]), GetValue<T3>(context, symbols[2]), GetValue<T4>(context, symbols[3]));
            return context.ExitCode;
        }
    }
}
