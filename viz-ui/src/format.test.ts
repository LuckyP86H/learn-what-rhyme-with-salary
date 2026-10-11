import { describe, expect, it } from "vitest";
import { fmtDuration, shortHost, shortName } from "./format";

describe("format", () => {
  it("formats durations", () => {
    expect(fmtDuration(0)).toBe("0s");
    expect(fmtDuration(4.4)).toBe("4s");
    expect(fmtDuration(125)).toBe("2m 05s");
    expect(fmtDuration(-3)).toBe("0s");
  });
  it("shortens task and host names", () => {
    expect(shortName("tasks.analyze_chunk")).toBe("analyze_chunk");
    expect(shortName(null)).toBe("(unknown task)");
    expect(shortHost("spot@a1b2c3d4e5f6")).toBe("spot·a1b2");
    expect(shortHost(null)).toBe("-");
  });
});
