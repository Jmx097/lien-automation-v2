import Bottleneck from "bottleneck";

export const limiter = new Bottleneck({
  minTime: 1200,
  maxConcurrent: 1
});