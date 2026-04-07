using ZincFlow.Core;

namespace ZincFlow.Fabric;

public sealed class AddAttribute : IProcessor
{
    private readonly string _key;
    private readonly string _value;

    public AddAttribute(string key, string value)
    {
        _key = key;
        _value = value;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        // WithAttribute uses overlay chain — zero Dictionary copy
        return SingleResult.Rent(FlowFile.WithAttribute(ff, _key, _value));
    }
}

public sealed class LogProcessor : IProcessor
{
    private readonly string _label;

    public LogProcessor(string label) => _label = label;

    public ProcessorResult Process(FlowFile ff)
    {
        return SingleResult.Rent(ff);
    }
}
