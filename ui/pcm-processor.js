// AudioWorklet: batches mic samples (Float32 at the context rate) and posts them to the page.
class PCMProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.bufferSize = 2048;             // ~43 ms at 48 kHz, ~21 ms after downsampling to 16 kHz
    this.buffer = new Float32Array(this.bufferSize);
    this.index = 0;
  }
  process(inputs) {
    const input = inputs[0];
    if (!input || !input.length) return true;
    const ch = input[0];
    for (let i = 0; i < ch.length; i++) {
      this.buffer[this.index++] = ch[i];
      if (this.index >= this.bufferSize) {
        this.port.postMessage(this.buffer.slice(0));
        this.index = 0;
      }
    }
    return true;
  }
}
registerProcessor("pcm-processor", PCMProcessor);
