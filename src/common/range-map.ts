import { Range, RangeInfo } from '../types/charcot.types'

/**
 * Specialized map that when queried for a numeric value it actually gives a range
 * to which such value belongs, for example given value '4' this map's get()
 * method returns range 4 - 6.
 * See the constructor documentation for more info.
 *
 * interval = 6, max = 12, start = 6
 * X,  0 => < 6
 * 6,  1 => 06 - 11
 * 12, 2 => 12 - 17
 * 18, 3 => 18 >
 *
 * interval = 6, max = 24, start = 12
 * X,  0 => < 12
 * 12, 1 => 12 - 17
 * 18, 2 => 18 - 23
 * 24, 3 => 24 >
 */
export default class RangeMap {
  private readonly interval: number
  private readonly max: number
  private readonly start: number
  private readonly map: Map<number, Range> = new Map<number, Range>()

  /**
   * Initializes a RangeMap object.
   *
   * @param interval - How big each range should be
   * @param max - The max value for which there's a range, for example if 90 then numbers higher than that fall into the 90+ range
   * @param start - The number at which to start ranges, for example if 6, numbers less than that just fall into the < 6 range
   */
  constructor(interval: number, max: number, start: number) {
    this.interval = interval
    this.max = max
    this.start = start

    // Map every single number between start (inclusive) and max (exclusive) to the range it belongs to
    let cur = start
    while (cur < this.max) {
      const first = cur
      const last = (cur += this.interval) - 1
      const range = `${first} - ${last}`
      for (let i = first; i <= last; i++) {
        this.map.set(i, range)
      }
    }
  }

  get(val: number): RangeInfo | undefined {
    const rangeInfo = { range: '', rank: val }
    if (val < this.start) {
      rangeInfo.range = `< ${this.start}`
    } else if (val >= this.max) {
      rangeInfo.range = `${this.max}+`
    } else {
      rangeInfo.range = this.map.get(val)!
    }
    return rangeInfo
  }
}
