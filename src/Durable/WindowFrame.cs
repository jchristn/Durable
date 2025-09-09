namespace Durable
{
    public class WindowFrame
    {
        public WindowFrameType Type { get; set; }
        public WindowFrameBound StartBound { get; set; } = new WindowFrameBound();
        public WindowFrameBound EndBound { get; set; } = new WindowFrameBound();
    }
}